#!/usr/bin/env bash
set -euo pipefail

# Publish every publishable workspace crate to crates.io.
#
# Usage:
#   scripts/publish.sh           # dry run: package and verify every crate
#   scripts/publish.sh --execute # publish for real
#
# `cargo publish --workspace` does the work this script used to hand-roll: it
# derives the topological order itself, polls the index after each upload so a
# dependent is never published before the version it pins is visible, and skips
# `publish = false` members — so `melin-example-counter` needs no exemption and
# a newly added crate cannot be forgotten by a list nobody updated.
#
# The one thing it does not do is skip a version already on crates.io, and a
# release needs that: publishes are permanent, so a run that dies at crate five
# of eight leaves four live forever, and re-running plain `--workspace` then
# fails on the first of them with nothing to do but finish by hand. The filter
# below restores resumability by excluding what is already published.
#
# Excluding is sound rather than merely convenient: a crate is excluded only
# because that exact version is on crates.io, so the dependents that pin it
# resolve it from the registry while the rest still resolve against the
# packages in the same run. If the registry copy and the local source ever
# disagree, package verification fails to compile and the release stops —
# which is the outcome we want over publishing an inconsistent set.

EXECUTE=0
case "${1:-}" in
    --execute) EXECUTE=1 ;;
    "") ;;
    # A typo like `--exec` silently meaning "dry run" is how someone comes to
    # believe they published when they did not.
    *) echo "usage: scripts/publish.sh [--execute]" >&2; exit 2 ;;
esac

cd "$(git rev-parse --show-toplevel)"

# The single source of truth for the release version; every member inherits it.
# Read with grep rather than `cargo metadata | jq` so the dry-run path, which
# CI runs, needs neither jq nor a metadata pass.
WORKSPACE_VERSION=$(grep -m1 '^version = "' Cargo.toml || true)
WORKSPACE_VERSION=${WORKSPACE_VERSION#version = \"}
WORKSPACE_VERSION=${WORKSPACE_VERSION%\"}
if [[ -z "$WORKSPACE_VERSION" ]]; then
    echo "error: could not read the workspace version from Cargo.toml" >&2
    exit 1
fi

# A published version that will not say what changed in it is one nobody can
# plan an upgrade around, and the entry cannot be added afterwards — crates.io
# is immutable. Checked in both modes, so CI's dry run catches a missing entry
# days before the release rather than during it, and checked here rather than
# in release.sh so a publish started by hand or from CI is guarded too.
if [[ ! -f CHANGELOG.md ]]; then
    echo "error: CHANGELOG.md is missing; every published version needs an entry" >&2
    exit 1
fi
# Keep a Changelog headings, e.g. `## [0.14.0] - 2026-08-20`. Only `.` is a
# metacharacter in a version made of digits and dots.
if ! grep -q "^## \[${WORKSPACE_VERSION//./\\.}\]" CHANGELOG.md; then
    echo "error: CHANGELOG.md has no '## [$WORKSPACE_VERSION]' entry" >&2
    echo "       add one before publishing; see https://keepachangelog.com" >&2
    exit 1
fi
echo "==> CHANGELOG.md has an entry for $WORKSPACE_VERSION"

# The dry run deliberately skips the already-published filter. It never
# uploads, so what is on crates.io is irrelevant to it, and CI runs it on main
# — where every crate *is* already published at the current version, so
# filtering would reduce the check to nothing. Verifying that all eight still
# package and build is the whole point. It also needs neither curl nor jq,
# which keeps the CI path free of both.
#
# `--allow-dirty` because this mode answers "does everything package and
# build", not "is the tree clean"; `release.sh` enforces a clean tree itself.
if (( ! EXECUTE )); then
    echo "==> Dry-run mode (pass --execute to publish for real)"
    cargo publish --workspace --dry-run --allow-dirty --locked
    echo
    echo "==> Done (nothing was uploaded)."
    exit 0
fi

echo "==> LIVE publish mode"

for tool in curl jq; do
    command -v "$tool" >/dev/null || { echo "error: $tool is required to publish" >&2; exit 1; }
done

# Where a crate's metadata lives in the sparse index, per
# https://doc.rust-lang.org/cargo/reference/registry-index.html
index_path() {
    local name="${1,,}"
    case ${#name} in
        1) printf '1/%s' "$name" ;;
        2) printf '2/%s' "$name" ;;
        3) printf '3/%s/%s' "${name:0:1}" "$name" ;;
        *) printf '%s/%s/%s' "${name:0:2}" "${name:2:2}" "$name" ;;
    esac
}

# Asked of the sparse index rather than of `cargo info`: inside a workspace,
# `cargo info <crate>@<version>` answers from the local path ("version: 0.13.0
# (from ./crates/core/app)"), and during a release the local version is by
# definition the version being released — so it would report every crate as
# already published and exclude the whole workspace.
index_has_version() {
    local name="$1" version="$2" response status body
    response=$(curl -sS --max-time 30 --retry 3 --retry-delay 2 \
        -w $'\n%{http_code}' "https://index.crates.io/$(index_path "$name")") || {
        echo "error: could not reach the crates.io index for $name" >&2
        exit 1
    }
    status="${response##*$'\n'}"
    body="${response%$'\n'*}"
    case "$status" in
        200) ;;
        404) return 1 ;;  # never published under this name at all
        *) echo "error: crates.io index returned HTTP $status for $name" >&2; exit 1 ;;
    esac
    # One JSON object per line, one per released version. `yanked` is not
    # consulted: a yanked version still occupies its slot, so re-publishing
    # over it fails just the same.
    [[ -n "$(jq -r --arg v "$version" 'select(.vers == $v) | .vers' <<< "$body")" ]]
}

echo
echo "==> Checking what is already on crates.io"

# `.publish` is absent (null) on a publishable crate and `[]` on one marked
# `publish = false`, so the select keeps exactly what cargo would publish.
PACKAGES=$(cargo metadata --no-deps --format-version 1 \
    | jq -r '.packages[] | select(.publish != []) | "\(.name)\t\(.version)"' | sort)

TOTAL=0
ALREADY=0
EXCLUDES=()
while IFS=$'\t' read -r name version; do
    TOTAL=$(( TOTAL + 1 ))
    if index_has_version "$name" "$version"; then
        echo "    already published, skipping: $name $version"
        EXCLUDES+=(--exclude "$name")
        ALREADY=$(( ALREADY + 1 ))
    fi
done <<< "$PACKAGES"

if (( TOTAL == 0 )); then
    echo "error: no publishable workspace crates found" >&2
    exit 1
fi

if (( ALREADY == TOTAL )); then
    echo
    echo "==> All $TOTAL crates are already published at this version; nothing to do."
    exit 0
fi
echo "    publishing $(( TOTAL - ALREADY )) of $TOTAL crates"

echo
echo "==> Publishing"
# `--locked` so the release is built against the dependency versions CI tested
# rather than whatever re-resolves at publish time; the lockfile is the record
# of what was actually verified. `${EXCLUDES[@]+...}` so an empty array does
# not trip `set -u` on bash < 4.4.
cargo publish --workspace --locked ${EXCLUDES[@]+"${EXCLUDES[@]}"}

echo
echo "==> Done."
