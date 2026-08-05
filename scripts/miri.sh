#!/usr/bin/env bash
set -euo pipefail

# Run Miri (the Rust UB interpreter) over the crates whose tests are
# FFI-free and I/O-free — the hand-rolled lock-free code where an
# aliasing or memory-ordering mistake is invisible to native tests.
#
# Scope:
#   - melin-pipeline: ring buffer, SPSC queue, seqlock — the entire
#     unsafe lock-free core. Full unit-test suite.
#   - melin-transport-core (--features no-persist): pipeline stage
#     logic with journal I/O compiled out. Test modules that exercise
#     real file/socket I/O are skipped (see SKIPS below) — Miri cannot
#     run those syscalls, and they add no UB coverage anyway.
#
# Out of scope (Miri cannot run them):
#   - melin-dpdk / melin-server-runtime: FFI and real sockets.
#   - melin-journal: O_DIRECT file I/O and io_uring syscalls.
#
# Usage:
#   scripts/miri.sh                  # full run
#   scripts/miri.sh spsc             # only tests matching "spsc"
#   MIRI_SEEDS=0..8 scripts/miri.sh  # also sweep scheduler seeds to
#                                    # explore more thread interleavings
#
# Requires the nightly toolchain with the miri component:
#   rustup component add --toolchain nightly miri

if ! cargo +nightly miri --version >/dev/null 2>&1; then
    echo "error: Miri is not installed." >&2
    echo "  rustup component add --toolchain nightly miri" >&2
    exit 1
fi

# Proptest under Miri: failure-persistence files need `getcwd`, which
# Miri's isolation blocks — disable them. Trim cases (native default is
# 256) because each case runs ~100x slower under Miri. Miri isolates
# the environment too, so the vars must be explicitly forwarded.
export PROPTEST_DISABLE_FAILURE_PERSISTENCE=1
export PROPTEST_CASES="${PROPTEST_CASES:-8}"
MIRIFLAGS="${MIRIFLAGS:-}"
MIRIFLAGS+=" -Zmiri-env-forward=PROPTEST_DISABLE_FAILURE_PERSISTENCE"
MIRIFLAGS+=" -Zmiri-env-forward=PROPTEST_CASES"

# Opt-in: re-run each test under several scheduler seeds so different
# thread interleavings are explored. Multiplies runtime by the seed
# count — off by default, worth a periodic deep run.
if [[ -n "${MIRI_SEEDS:-}" ]]; then
    MIRIFLAGS+=" -Zmiri-many-seeds=$MIRI_SEEDS"
fi
export MIRIFLAGS

FILTER=()
if [[ -n "${1:-}" ]]; then
    FILTER=("$1")
fi

echo "==> MIRIFLAGS=$MIRIFLAGS"

echo "==> miri: melin-pipeline"
cargo +nightly miri test -p melin-pipeline -- "${FILTER[@]}"

# Test modules that hit real file/socket syscalls even under
# no-persist (pipeline_tests: every pipeline needs a real journal file
# at construction; no-persist only skips the writes). Keep in sync with
# any new I/O-touching test modules.
SKIPS=(
    --skip health::
    --skip snapshot::
    --skip shadow::
    --skip journaled_app::
    --skip pipeline_tests::
    --skip replication::handoff_test
    --skip replication::validate
    --skip replication::archive
    --skip replication::catchup
)

echo "==> miri: melin-transport-core (no-persist)"
cargo +nightly miri test -p melin-transport-core --features no-persist --lib -- "${SKIPS[@]}" "${FILTER[@]}"

echo "==> Miri passed"
