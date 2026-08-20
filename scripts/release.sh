#!/usr/bin/env bash
set -euo pipefail

# Cut a release: bump the workspace version, stamp the BSL Change Date into
# every LICENSE copy, commit on a release branch, tag, push, and publish.
#
# Usage:
#   scripts/release.sh 0.14.0                    # rehearsal (see below)
#   scripts/release.sh 0.14.0 --execute          # for real: pushes and publishes
#   scripts/release.sh 0.14.0 --change-date 2030-01-01
#   scripts/release.sh 0.14.0 --execute --yes    # skip the confirmation prompt
#   scripts/release.sh 0.14.0 --keep             # rehearsal, keep what it built
#
# Without `--execute` this is a full rehearsal rather than a preview: it makes
# the branch, the edits, the commit (so the pre-commit hook runs the real
# release gate — fmt, clippy, the feature builds, the suite) and the tag, runs
# `publish.sh` in its dry-run mode, and then puts the repository back exactly
# as it found it. Nothing leaves the machine. `--keep` skips the restore when
# you want to inspect the result; a rehearsal that *fails* also keeps
# everything, so there is something left to debug, and prints how to clean up.
#
# What it deliberately does not do:
#
#   * Merge to main. The tag is cut on `release/vX.Y.Z` and the human merges
#     it — this repository fast-forwards, so the tagged commit becomes main's
#     tip unchanged. Do it promptly: the up-to-date check below only proves
#     main was current when the release started.
#   * Verify a CHANGELOG entry exists for the version. That check belongs to
#     `publish.sh` per the roadmap, so that it also guards a publish run
#     started by hand or from CI, not just one driven from here.

# --- Arguments ---------------------------------------------------------------

NEW_VERSION=""
EXECUTE=0
KEEP=0
ASSUME_YES=0
CHANGE_DATE=""

usage() {
    echo "usage: scripts/release.sh <version> [--execute] [--change-date YYYY-MM-DD] [--yes] [--keep]" >&2
    exit 2
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --execute) EXECUTE=1; shift ;;
        --keep) KEEP=1; shift ;;
        --yes|-y) ASSUME_YES=1; shift ;;
        --change-date) CHANGE_DATE="${2:-}"; [[ -n "$CHANGE_DATE" ]] || usage; shift 2 ;;
        -h|--help) usage ;;
        -*) echo "error: unknown option '$1'" >&2; usage ;;
        *)
            [[ -z "$NEW_VERSION" ]] || { echo "error: version given twice" >&2; usage; }
            NEW_VERSION="$1"; shift ;;
    esac
done

[[ -n "$NEW_VERSION" ]] || usage

# Plain X.Y.Z only. Pre-release suffixes are rejected rather than half-handled:
# `sort -V` below orders `0.14.0-rc.1` *after* `0.14.0`, which is backwards
# under semver, so accepting them would mean an ordering check that lies.
if [[ ! "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "error: '$NEW_VERSION' is not a plain X.Y.Z version" >&2
    exit 1
fi

BRANCH="release/v$NEW_VERSION"
TAG="v$NEW_VERSION"

# The BSL Change Date. Default is four years out, which is what the licence
# text falls back to anyway ("the fourth anniversary of the first publicly
# available distribution") — so the default changes nothing legally and only
# removes the placeholder. Override to convert sooner; converting later than
# the fallback is not possible.
if [[ -z "$CHANGE_DATE" ]]; then
    CHANGE_DATE=$(date -u -d "+4 years" +%F) || {
        echo "error: GNU date required (for '-d +4 years'); pass --change-date instead" >&2
        exit 1
    }
fi
if [[ ! "$CHANGE_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "error: --change-date must be YYYY-MM-DD, got '$CHANGE_DATE'" >&2
    exit 1
fi

# --- Failure guidance --------------------------------------------------------

# Tracked so the exit trap can say what actually happened. A release that dies
# after the push is a very different situation from one that dies before it,
# and the difference is not recoverable from the exit code.
DID_BRANCH=0
DID_COMMIT=0
DID_TAG=0
DID_PUSH=0
DID_PUBLISH_START=0
ORIGINAL_BRANCH=""

cleanup_hint() {
    echo "    Undo the local work with:"
    (( DID_TAG )) && echo "      git tag -d $TAG"
    echo "      git checkout ${ORIGINAL_BRANCH:-main}"
    (( DID_BRANCH )) && echo "      git branch -D $BRANCH"
    return 0
}

on_exit() {
    local status=$?
    (( status == 0 )) && return 0
    echo >&2
    echo "==> release.sh failed (exit $status)." >&2
    if (( DID_PUBLISH_START )); then
        echo "    Publishing had already begun — some crates may be live on crates.io." >&2
        echo "    crates.io publishes are permanent: do NOT retry under a new version." >&2
        echo "    Resume with 'scripts/publish.sh --execute'; it skips crates already published." >&2
    elif (( DID_PUSH )); then
        echo "    $BRANCH and $TAG are already on origin — left in place." >&2
        echo "    Resume with 'scripts/publish.sh --execute' once the failure is understood." >&2
    else
        echo "    Nothing left this machine." >&2
        cleanup_hint >&2
    fi
} >&2
trap on_exit EXIT

step() { echo; echo "==> $*"; }

# `grep -c` exits 1 on no match, which `set -e` would turn into an abort — but
# "zero matches" is an answer these checks need, not a failure.
count() { grep -c -- "$1" "$2" || true; }

# --- Preconditions -----------------------------------------------------------

cd "$(git rev-parse --show-toplevel)"

if (( EXECUTE )); then
    echo "==> LIVE release mode — this will push and publish"
else
    echo "==> Rehearsal mode (pass --execute to release for real)"
fi

step "Checking the working tree"
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "error: working tree has uncommitted changes; commit or stash first" >&2
    exit 1
fi
if [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
    echo "warning: untracked files present; they will not be part of the release" >&2
fi

ORIGINAL_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ "$ORIGINAL_BRANCH" != "main" ]]; then
    echo "error: releases are cut from main, currently on '$ORIGINAL_BRANCH'" >&2
    exit 1
fi

step "Checking main is current"
git fetch --quiet origin main
if [[ "$(git rev-parse HEAD)" != "$(git rev-parse FETCH_HEAD)" ]]; then
    echo "error: local main differs from origin/main; pull (or push) first" >&2
    exit 1
fi

step "Checking $BRANCH and $TAG are free"
if git rev-parse --verify --quiet "refs/heads/$BRANCH" >/dev/null; then
    echo "error: branch $BRANCH already exists locally" >&2
    exit 1
fi
if git rev-parse --verify --quiet "refs/tags/$TAG" >/dev/null; then
    echo "error: tag $TAG already exists locally" >&2
    exit 1
fi
if git ls-remote --exit-code --heads origin "refs/heads/$BRANCH" >/dev/null 2>&1; then
    echo "error: branch $BRANCH already exists on origin" >&2
    exit 1
fi
if git ls-remote --exit-code --tags origin "refs/tags/$TAG" >/dev/null 2>&1; then
    echo "error: tag $TAG already exists on origin — $NEW_VERSION is already released" >&2
    exit 1
fi

step "Checking the lockfile resolves"
cargo metadata --locked --format-version 1 >/dev/null

# The single source of truth is `[workspace.package] version` in the root
# manifest; every member inherits it. Read it from there rather than from
# cargo metadata, because that is the line the bump has to rewrite.
OLD_VERSION=$(grep -m1 '^version = "' Cargo.toml || true)
OLD_VERSION=${OLD_VERSION#version = \"}
OLD_VERSION=${OLD_VERSION%\"}
if [[ -z "$OLD_VERSION" ]]; then
    echo "error: could not read the current version from Cargo.toml" >&2
    exit 1
fi

if [[ "$OLD_VERSION" == "$NEW_VERSION" ]]; then
    echo "error: workspace is already at $NEW_VERSION" >&2
    exit 1
fi
if ! printf '%s\n%s\n' "$OLD_VERSION" "$NEW_VERSION" | sort -V -C; then
    echo "error: $NEW_VERSION is not greater than the current $OLD_VERSION" >&2
    exit 1
fi

echo
echo "    version:     $OLD_VERSION -> $NEW_VERSION"
echo "    branch:      $BRANCH"
echo "    tag:         $TAG"
echo "    Change Date: $CHANGE_DATE"

if (( EXECUTE )) && (( ! ASSUME_YES )); then
    if [[ ! -t 0 ]]; then
        echo "error: --execute needs a terminal to confirm on; pass --yes" >&2
        exit 1
    fi
    echo
    read -r -p "Push $TAG and publish $NEW_VERSION to crates.io? Publishes are permanent. [y/N] " reply
    if [[ "$reply" != "y" && "$reply" != "Y" ]]; then
        echo "Aborted."
        exit 1
    fi
fi

# --- Branch ------------------------------------------------------------------

step "Creating $BRANCH"
git checkout -q -b "$BRANCH"
DID_BRANCH=1

# --- Version bump ------------------------------------------------------------

# The root manifest carries the version in nine places — the inherited
# `[workspace.package]` one plus a pin per intra-workspace dependency — and
# they must move in lockstep: a dependent resolving against a version its
# provider no longer has is a broken publish, discovered halfway through.
#
# So the substitution is counted at every step rather than trusted. Anything
# unexpected aborts instead of being silently half-applied, which is the whole
# risk of rewriting a manifest with a regex.
step "Bumping the workspace version"

# Only `.` is a regex metacharacter in a version string the X.Y.Z check above
# has already proven to be digits and dots.
OLD_RE=${OLD_VERSION//./\\.}
NEW_RE=${NEW_VERSION//./\\.}

WS_AT_OLD='^version = "'"$OLD_RE"'"$'
DEP_AT_OLD='^melin-[A-Za-z0-9_-]* = { path = "[^"]*", version = "'"$OLD_RE"'"'
# Deliberately looser than the substitution shape, so a pin written differently
# is caught here rather than quietly skipped by the sed below.
DEP_ANY='^melin-[A-Za-z0-9_-]* = .*version = "'

STALE=$(grep -n -- "$DEP_ANY" Cargo.toml | grep -v -- "version = \"$OLD_RE\"" || true)
if [[ -n "$STALE" ]]; then
    echo "error: intra-workspace pins are not all at $OLD_VERSION; fix them first:" >&2
    echo "$STALE" >&2
    exit 1
fi

WS_BEFORE=$(count "$WS_AT_OLD" Cargo.toml)
DEP_BEFORE=$(count "$DEP_AT_OLD" Cargo.toml)
DEP_TOTAL=$(count "$DEP_ANY" Cargo.toml)

if (( WS_BEFORE != 1 )); then
    echo "error: expected 1 [workspace.package] version line at $OLD_VERSION, found $WS_BEFORE" >&2
    exit 1
fi
if (( DEP_BEFORE != DEP_TOTAL )); then
    echo "error: only $DEP_BEFORE of $DEP_TOTAL intra-workspace pins match the rewrite shape" >&2
    exit 1
fi

sed -i \
    -e "s/^version = \"$OLD_RE\"\$/version = \"$NEW_VERSION\"/" \
    -e "s/^\\(melin-[A-Za-z0-9_-]* = { path = \"[^\"]*\", version = \\)\"$OLD_RE\"/\\1\"$NEW_VERSION\"/" \
    Cargo.toml

# Scoped to those two shapes on purpose: a third-party dependency that happens
# to sit at the same version number is not our problem and must not block a
# release.
LEFT=$(( $(count "$WS_AT_OLD" Cargo.toml) + $(count "$DEP_AT_OLD" Cargo.toml) ))
if (( LEFT != 0 )); then
    echo "error: $LEFT version string(s) still at $OLD_VERSION after the bump" >&2
    exit 1
fi

WS_AFTER=$(count '^version = "'"$NEW_RE"'"$' Cargo.toml)
DEP_AFTER=$(count '^melin-[A-Za-z0-9_-]* = { path = "[^"]*", version = "'"$NEW_RE"'"' Cargo.toml)
if (( WS_AFTER != WS_BEFORE || DEP_AFTER != DEP_BEFORE )); then
    echo "error: rewrote $WS_AFTER+$DEP_AFTER version strings, expected $WS_BEFORE+$DEP_BEFORE" >&2
    exit 1
fi
echo "    rewrote $(( WS_AFTER + DEP_AFTER )) version strings in Cargo.toml"

# `--workspace` restricts the update to workspace members, so a release cannot
# quietly drag in new third-party versions along with the bump.
step "Refreshing Cargo.lock"
cargo update --quiet --workspace
cargo metadata --locked --format-version 1 >/dev/null

# --- BSL Change Date ---------------------------------------------------------

# The root LICENSE and one byte-identical copy per crate — cargo packages files
# under a package root automatically, and BSL requires the licence on every
# copy of the Licensed Work, so a published crate without its own copy would
# not satisfy our own terms. They are stamped by rewriting the root and copying
# it out, which makes them identical by construction rather than by nine
# separate edits that have to agree.
step "Stamping the BSL Change Date"

# `mapfile` from a process substitution cannot fail the script under `set -e`,
# so an empty result is checked for explicitly below — silently falling back to
# stamping the root LICENSE alone would ship crates carrying a stale date.
# `.publish` is absent (null) on a publishable crate and `[]` on one marked
# `publish = false`, so the comparison sorts them without a special case.
mapfile -t CRATE_DIRS < <(
    cargo metadata --no-deps --format-version 1 | jq -r '
        .workspace_root as $root
        | .packages[]
        | [(.publish != []),
           (.manifest_path | ltrimstr($root + "/") | rtrimstr("/Cargo.toml"))]
        | @tsv'
)

if (( ${#CRATE_DIRS[@]} == 0 )); then
    echo "error: cargo metadata listed no workspace members" >&2
    exit 1
fi

LICENSES=(LICENSE)
for entry in "${CRATE_DIRS[@]}"; do
    publishable="${entry%%$'\t'*}"
    dir="${entry#*$'\t'}"
    if [[ -f "$dir/LICENSE" ]]; then
        LICENSES+=("$dir/LICENSE")
    elif [[ "$publishable" == "true" ]]; then
        echo "error: $dir has no LICENSE; every published crate must carry one" >&2
        exit 1
    fi
done

# Divergence here means a copy was edited by hand at some point. Catch it
# before overwriting, so the difference is inspected rather than erased.
ROOT_SUM=$(sha256sum LICENSE | cut -d' ' -f1)
for license in "${LICENSES[@]:1}"; do
    if [[ "$(sha256sum "$license" | cut -d' ' -f1)" != "$ROOT_SUM" ]]; then
        echo "error: $license differs from the root LICENSE; reconcile them first" >&2
        exit 1
    fi
done

# Anchored to the start of the line so the two places the licence *prose*
# mentions the Change Date are left alone.
DATE_BEFORE=$(count '^Change Date:[[:space:]]' LICENSE)
if (( DATE_BEFORE != 1 )); then
    echo "error: expected exactly 1 'Change Date:' line in LICENSE, found $DATE_BEFORE" >&2
    exit 1
fi

# The parameter block aligns its values at column 23; keep that.
sed -i "s/^Change Date:[[:space:]].*\$/Change Date:          $CHANGE_DATE/" LICENSE

if (( $(count "^Change Date:          $CHANGE_DATE\$" LICENSE) != 1 )); then
    echo "error: Change Date was not stamped into LICENSE" >&2
    exit 1
fi

for license in "${LICENSES[@]:1}"; do
    cp LICENSE "$license"
done
echo "    stamped $CHANGE_DATE into ${#LICENSES[@]} LICENSE files"

# --- Commit and tag ----------------------------------------------------------

# No --no-verify: the pre-commit hook is the release gate. It runs fmt, clippy,
# the off-by-default feature builds, and the suite, and a release is exactly
# the commit that must not skip them.
step "Committing"
git add Cargo.toml Cargo.lock "${LICENSES[@]}"
git commit -q -m "chore(release): v$NEW_VERSION

Bump the workspace to $NEW_VERSION and set the BSL Change Date to
$CHANGE_DATE across the root LICENSE and its per-crate copies."
DID_COMMIT=1

step "Tagging $TAG"
git tag -a "$TAG" -m "Melin $TAG"
DID_TAG=1

# --- Push and publish --------------------------------------------------------

if (( EXECUTE )); then
    # --atomic so the branch and the tag land together: a tag on origin whose
    # commit is not there is worse than neither.
    step "Pushing $BRANCH and $TAG"
    git push --atomic origin "$BRANCH" "$TAG"
    DID_PUSH=1

    step "Publishing to crates.io"
    DID_PUBLISH_START=1
    scripts/publish.sh --execute
else
    step "Skipping push (rehearsal)"

    # The same script the live path runs, one mode over. Its dry run resolves
    # the `melin-* = "$NEW_VERSION"` pins against the other packages in the
    # same run rather than against the registry, which is what lets a version
    # that has never been released be rehearsed at all.
    step "Packaging every crate (publish dry run)"
    scripts/publish.sh
fi

# --- Done --------------------------------------------------------------------

echo
if (( EXECUTE )); then
    echo "==> Released v$NEW_VERSION."
    echo "    Merge it — the tag is on $BRANCH, not yet on main:"
    echo "      git checkout main && git merge --ff-only $BRANCH && git push origin main"
elif (( KEEP )); then
    echo "==> Rehearsal complete; $BRANCH and $TAG kept as asked."
    cleanup_hint
else
    git checkout -q "$ORIGINAL_BRANCH"
    git branch -q -D "$BRANCH"
    git tag -d "$TAG" >/dev/null
    echo "==> Rehearsal complete; repository restored to $ORIGINAL_BRANCH."
    echo "    Re-run with --execute to release v$NEW_VERSION for real."
fi
