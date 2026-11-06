# Output digest: the fold's cost (measurement, 2026-11-06)

Settles open question 1 of the
[divergence-detection plan](divergence-detection-2026-10.md): the hash and
the window size `K`.

**Hardware:** Apple M2 (Asahi Linux, aarch64), one core, nothing pinned.
**Build:** `--release` (fat LTO, one codegen unit), `twox-hash` 2.1.5 with
`std` (NEON stripe loop), `blake3` 1.x.
**Command:** `cargo run --release -p melin-transport-core --example
fold_bench` (4 million reports per run, one report per position, the
median of five runs). Bodies cycle through a pool made before the clock
starts.

## Results, ns per report

The real `WindowFolder` (XXH3-128, `K` = 64), fold, close, chain and board
store included:

| Body | ns |
| --- | --- |
| 8 B | 6.30 |
| 24 B | 6.86 |
| 300 B | 31.52 |

A window with no report (header, finalize, chain step, board store):
27.13 ns per window.

The stand-in fold (the same bytes and chain step, no board store) per hash
and `K`:

| Hash | `K` | 8 B | 24 B | 300 B |
| --- | --- | --- | --- | --- |
| XXH3-128 | 16 | 8.24 | 9.06 | 34.05 |
| XXH3-128 | 64 | 6.00 | 6.58 | 31.57 |
| XXH3-128 | 256 | 5.78 | 6.33 | 31.01 |
| BLAKE3 | 16 | 51.28 | 64.80 | 399.62 |
| BLAKE3 | 64 | 43.71 | 58.25 | 393.73 |
| BLAKE3 | 256 | 41.54 | 56.04 | 393.37 |

## Decision

- **XXH3-128.** BLAKE3 costs six to thirteen times more per report, up to
  about 400 ns for a 300-byte body: four times the per-event budget on
  the response thread alone, for a collision resistance the check does
  not need.
- **`K` = 64.** From 16 to 64 saves about 2 ns per report on small
  bodies; from 64 to 256 under half a nanosecond, while the idle detection
  delay grows fourfold. The per-window cost amortised over 64 positions is
  under half a nanosecond.
- **`std` on.** Not measured without it: `std` turns on the NEON and AVX2
  stripe loops for windows past 240 bytes, and costs nothing in a crate
  that links `std`.

Not measured here: the fold inside a running node, where the response
thread's tail is what matters. That end-to-end measurement is deferred
with the plan's other measurements.
