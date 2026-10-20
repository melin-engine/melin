#!/usr/bin/env bash
# Tests for server-setup.sh's kernel-parameter verification, for the
# choice of the journal disk, and for the udev rule that tunes it and the
# read-back that checks it.
#
# This is the half of the setup script that has no other safety net: it
# is what stands between "the params were applied" and the silent no-op
# that shipped before it (a `sed` for `GRUB_CMDLINE_LINUX_DEFAULT` on an
# image that only defines `GRUB_CMDLINE_LINUX` matched nothing, yet the
# script still ran update-grub and announced success). A verifier that
# is itself wrong would re-introduce exactly that failure, so it gets
# tested directly.
#
# The function bodies are extracted from server-setup.sh and sourced, so
# these tests exercise the shipping code rather than a copy of it.
#
# Usage: ./scripts/bench/test-server-setup.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SETUP_SCRIPT="${SCRIPT_DIR}/server-setup.sh"

if [[ ! -f "$SETUP_SCRIPT" ]]; then
    echo "error: cannot find server-setup.sh at $SETUP_SCRIPT" >&2
    exit 1
fi

TMP_ROOT=$(mktemp -d)
trap 'rm -rf "$TMP_ROOT"' EXIT

# Pull the functions under test, and the journal tuning list two of them
# share, out of the setup script. Sourcing the whole script is not an
# option: it is a root-only provisioner that would start installing
# packages. Anchoring on `^name() {` .. `^}` works because the file is
# shfmt-shaped — the closing brace of a top-level function is the only
# `}` in column 0 within its range, as the closing parenthesis of a
# top-level array is the only `)` in column 0 within its.
extract_fn() {
    awk "/^$1\(\) \{/,/^\}/" "$SETUP_SCRIPT"
}

extract_array() {
    awk "/^$1=\(/,/^\)/" "$SETUP_SCRIPT"
}

FN_FILE="${TMP_ROOT}/fns.sh"
{
    extract_fn kernel_param_live
    extract_fn kernel_param_in_cfg
    extract_fn kernel_param_values_in_cfg
    extract_fn resolve_physical_cores
    extract_fn write_grub_dropin
    extract_fn verify_kernel_params
    extract_fn write_journal_udev_rule
    extract_fn verify_journal_queue_tuning
    extract_fn fstab_journal_uuid
    extract_fn pick_journal_disk
    extract_array JOURNAL_QUEUE_TUNING
} > "$FN_FILE"

for fn in kernel_param_live kernel_param_in_cfg kernel_param_values_in_cfg \
    resolve_physical_cores write_grub_dropin verify_kernel_params \
    write_journal_udev_rule verify_journal_queue_tuning \
    fstab_journal_uuid pick_journal_disk; do
    if ! grep -q "^${fn}() {" "$FN_FILE"; then
        echo "error: failed to extract ${fn} from server-setup.sh — has it been" >&2
        echo "  renamed or reformatted? These tests are now vacuous; fix the" >&2
        echo "  extraction before trusting a pass." >&2
        exit 1
    fi
done

# shellcheck source=/dev/null
source "$FN_FILE"

if ! declare -p JOURNAL_QUEUE_TUNING >/dev/null 2>&1 \
    || [[ "${#JOURNAL_QUEUE_TUNING[@]}" -eq 0 ]]; then
    echo "error: failed to extract JOURNAL_QUEUE_TUNING from server-setup.sh —" >&2
    echo "  has it been renamed or reformatted? The journal tuning tests are" >&2
    echo "  now vacuous; fix the extraction before trusting a pass." >&2
    exit 1
fi

PASS=0
FAIL=0

ok() {
    PASS=$((PASS + 1))
    echo "  ok — $1"
}

bad() {
    FAIL=$((FAIL + 1))
    echo "  FAIL — $1" >&2
    [[ -n "${2:-}" ]] && echo "         $2" >&2
}

# Point every global the extracted functions read at this case's temp
# files. All five matter, not just the ones a given case exercises: the
# error paths interpolate GRUB_FILE and GRUB_DROPIN into their closing
# lines, and under `set -u` an unset one kills the function mid-message.
# That aborts before the `return 1` the case means to be checking, while
# still emitting enough of the text for a lenient grep to pass — a green
# test for a function that crashed.
case_globals() {
    local name="$1"
    GRUB_CFG="${TMP_ROOT}/${name}.cfg"
    CMDLINE_SOURCE="${TMP_ROOT}/${name}.cmdline"
    REBOOT_FLAG="${TMP_ROOT}/${name}.reboot"
    GRUB_DROPIN="${TMP_ROOT}/${name}.dropin"
    GRUB_FILE="${TMP_ROOT}/${name}.default-grub"
    rm -f "$REBOOT_FLAG"
}

# Build a fake grub.cfg carrying `params` on a linux line, and a fake
# /proc/cmdline carrying `live`. Sets the globals the functions read.
setup_case() {
    local params="$1" live="$2" name="$3"
    case_globals "$name"
    printf '\tlinux\t/boot/vmlinuz-test root=UUID=abc ro %s\n' "$params" > "$GRUB_CFG"
    printf 'BOOT_IMAGE=/boot/vmlinuz-test root=UUID=abc ro %s\n' "$live" > "$CMDLINE_SOURCE"
}

# Source `file` in a child shell with GRUB_CMDLINE_LINUX preset to
# `vendor`, and print what the variable ends up as — exactly what
# grub-mkconfig does when it reaches the drop-in.
source_dropin() {
    local file="$1" vendor="$2"
    GRUB_CMDLINE_LINUX="$vendor" bash -c '. "$1"; printf "%s" "$GRUB_CMDLINE_LINUX"' _ "$file"
}

echo "=== server-setup.sh core detection ==="

# ---------------------------------------------------------------------
# The normal path: lscpu collapses SMT siblings and its answer is used
# as-is. 12 cores on a 24-thread host means isolating 1-11.
# ---------------------------------------------------------------------
if got=$(resolve_physical_cores 12 24 2>/dev/null) && [[ "$got" == "12" ]]; then
    ok "lscpu's count is used when it is usable"
else
    bad "expected 12 from a working lscpu" "got: [${got}]"
fi

# ---------------------------------------------------------------------
# Fallback. This must be `nproc --all`, never plain `nproc`: on a host
# already tuned by this script, isolcpus pins default affinity to core 0
# and plain `nproc` answers 1 (measured: 1 vs 24 on a 12-core box booted
# with isolcpus=1-11). The caller passes --all; this checks the value is
# actually taken.
# ---------------------------------------------------------------------
if got=$(resolve_physical_cores 0 24 2>/dev/null) && [[ "$got" == "24" ]]; then
    ok "a failed lscpu falls back to the nproc --all count"
else
    bad "expected the fallback count of 24" "got: [${got}]"
fi

if err=$(resolve_physical_cores 0 24 2>&1 >/dev/null) && grep -q "nproc --all" <<< "$err"; then
    ok "the fallback warns, and names the source it used"
else
    bad "falling back silently hides a detection failure" "stderr: [${err}]"
fi

# ---------------------------------------------------------------------
# The bug this guard exists for. One core means LAST_ISOLATED=0 and an
# `isolcpus=nohz,domain,1-0` that the kernel cannot act on — and that
# verify_kernel_params would pass, since the malformed token does reach
# grub.cfg and /proc/cmdline verbatim. Refuse instead.
# ---------------------------------------------------------------------
if out=$(resolve_physical_cores 1 1 2>&1); then
    bad "a single-core host must be refused, not isolated" "got: [${out}]"
elif grep -q "at least two" <<< "$out"; then
    ok "a single usable core is refused rather than isolated"
else
    bad "the refusal should explain the two-core minimum" "$out"
fi

# The refusal has to survive both sources disagreeing, since that is the
# shape a genuinely tiny host takes: lscpu says 1, nproc --all agrees.
for pair in "0 1" "1 0" "0 0"; do
    # shellcheck disable=SC2086
    if resolve_physical_cores $pair >/dev/null 2>&1; then
        bad "resolve_physical_cores($pair) must fail, not return a count"
    fi
done
ok "every sub-two combination is refused"

# The `--all` is chosen at the call site, which the tests above cannot
# reach — they hand the function numbers directly. Pin it in the source
# instead: every `nproc` command substitution in the setup script must
# carry `--all`, or a tuned host silently reports one core again. This
# asserts on text rather than behaviour, which is weak, but a top-level
# command choice has nowhere else to be caught.
nproc_calls=$(grep -c '\$(nproc' "$SETUP_SCRIPT")
nproc_all_calls=$(grep -c '\$(nproc --all' "$SETUP_SCRIPT")
if [[ "$nproc_calls" -gt 0 && "$nproc_calls" -eq "$nproc_all_calls" ]]; then
    ok "every nproc call in server-setup.sh passes --all"
else
    bad "a bare \$(nproc) reports 1 on an isolcpus host" \
        "\$(nproc: ${nproc_calls}, \$(nproc --all: ${nproc_all_calls})"
fi

# The property behind all of the above: whatever comes back is never a
# count that would build a reversed range.
range_ok=1
for pair in "12 24" "0 24" "2 2" "4 8"; do
    # shellcheck disable=SC2086
    got=$(resolve_physical_cores $pair 2>/dev/null) || continue
    [[ $((got - 1)) -ge 1 ]] || range_ok=0
done
if [[ "$range_ok" -eq 1 ]]; then
    ok "an accepted count never yields a reversed isolcpus range"
else
    bad "some accepted count produces 1-0 or narrower"
fi

echo ""
echo "=== server-setup.sh drop-in generation ==="

# ---------------------------------------------------------------------
# The escape in `\${GRUB_CMDLINE_LINUX}` has to survive into the file so
# that grub-mkconfig expands it and our params are appended. Lose the
# backslash and the drop-in replaces the vendor's command line instead —
# on hosting images that discards the serial console settings, which on a
# remote box is the way back in. Nothing else in this file would notice.
# ---------------------------------------------------------------------
BENCH_PARAMS="isolcpus=nohz,domain,1-5 nosmt iommu=pt"
DROPIN="${TMP_ROOT}/dropin.d/99-melin-ec-bench.cfg"
write_grub_dropin "$DROPIN"

if [[ -f "$DROPIN" ]]; then
    ok "drop-in written, creating its directory on the way"
else
    bad "write_grub_dropin did not create $DROPIN"
fi

got=$(source_dropin "$DROPIN" "quiet console=ttyS0,115200")
if [[ "$got" == "quiet console=ttyS0,115200 ${BENCH_PARAMS}" ]]; then
    ok "vendor command line is preserved and the params appended"
else
    bad "the drop-in must append, not replace" "got: [${got}]"
fi

# A vendor file that never defines the variable is the case that broke the
# old script. Appending to nothing is fine; silently doing nothing is not.
got=$(source_dropin "$DROPIN" "")
if [[ "$got" == *"isolcpus=nohz,domain,1-5"* && "$got" == *"nosmt"* ]]; then
    ok "params still land when the vendor defines no command line"
else
    bad "an undefined GRUB_CMDLINE_LINUX must still yield the params" "got: [${got}]"
fi

# Regenerating is what makes the setup step idempotent: a second run must
# rewrite the file, never append a second copy of the parameters.
before=$(cat "$DROPIN")
write_grub_dropin "$DROPIN"
if [[ "$(cat "$DROPIN")" == "$before" ]]; then
    ok "rewriting the drop-in is idempotent"
else
    bad "a second write changed the file — params are accumulating"
fi

# A changed core count has to be reflected, since the whole point of
# regenerating is that a resized host re-isolates the right range.
BENCH_PARAMS="isolcpus=nohz,domain,1-11 nosmt"
write_grub_dropin "$DROPIN"
got=$(source_dropin "$DROPIN" "quiet")
if [[ "$got" == "quiet ${BENCH_PARAMS}" ]]; then
    ok "a changed parameter set replaces the previous one"
else
    bad "regenerating must pick up the new params" "got: [${got}]"
fi

echo ""
echo "=== server-setup.sh journal udev rule ==="

# ---------------------------------------------------------------------
# The rule must find the journal disk by its filesystem, never by kernel
# name: NVMe names follow probe order, and on a host with two identical
# drives they swapped across the setup's own reboot, so a rule keyed on
# `nvme0n1` tuned the OS disk and left the journal untuned.
# ---------------------------------------------------------------------
UUID_A="38781855-bba1-4d2e-b375-e7619b2edfed"
RULE="${TMP_ROOT}/99-melin-journal-nvme.rules"
write_journal_udev_rule "$RULE" "$UUID_A"
rule_line=$(grep '^ACTION' "$RULE")

if [[ "$rule_line" == *"ENV{ID_FS_UUID}==\"${UUID_A}\""* \
    && "$rule_line" == *'ENV{DEVTYPE}=="disk"'* ]]; then
    ok "the rule matches the journal disk by its filesystem UUID"
else
    bad "the rule must match on the filesystem UUID, whole disks only" "$rule_line"
fi
if grep -q 'KERNEL==' "$RULE"; then
    bad "the rule must not match on a kernel name, which can change at boot" "$rule_line"
else
    ok "the rule does not depend on the disk's kernel name"
fi

# Every setting in the list, and only one line carrying them: the rule and
# the setup's read-back iterate the same list, so a setting missing here is
# one the read-back would then report as never applied.
missing=()
for param in "${JOURNAL_QUEUE_TUNING[@]}"; do
    [[ "$rule_line" == *"ATTR{queue/${param% *}}=\"${param#* }\""* ]] || missing+=("$param")
done
if [[ "${#missing[@]}" -eq 0 && $(grep -c '^ACTION' "$RULE") -eq 1 ]]; then
    ok "every tuning setting is in the rule, on one line"
else
    bad "the rule is missing settings: ${missing[*]}" "$(cat "$RULE")"
fi

# udev applies the assignments left to right, and switching the scheduler
# resets nr_requests: set after it, the scheduler would undo the queue
# depth the rule just asked for.
first_attr=""
[[ "$rule_line" =~ ATTR\{queue/([a-z_]+)\} ]] && first_attr="${BASH_REMATCH[1]}"
if [[ "$first_attr" == "scheduler" ]]; then
    ok "the rule sets the scheduler before anything it resets"
else
    bad "the scheduler must be the rule's first setting" "$rule_line"
fi

# The kernel raises an nr_requests below its floor of 4 (BLKDEV_MIN_RQ)
# without a word, and the read-back would then fail every setup.
below_floor=()
for param in "${JOURNAL_QUEUE_TUNING[@]}"; do
    [[ "${param% *}" == "nr_requests" && "${param#* }" -lt 4 ]] && below_floor+=("$param")
done
if [[ "${#below_floor[@]}" -eq 0 ]]; then
    ok "nr_requests asks for no less than the kernel's floor"
else
    bad "nr_requests below 4 is silently raised" "${below_floor[*]}"
fi

# Rewriting must replace, as with the grub drop-in: a second run on a
# reformatted disk must not leave a rule for the old filesystem behind.
UUID_B="54f81ef7-ed20-4662-a1f0-f9b976d41880"
write_journal_udev_rule "$RULE" "$UUID_B"
if grep -q "$UUID_B" "$RULE" && ! grep -q "$UUID_A" "$RULE"; then
    ok "rewriting the rule replaces the previous disk's"
else
    bad "a rewrite must leave only the new UUID" "$(cat "$RULE")"
fi

# An empty UUID would make `ENV{ID_FS_UUID}==""`, which matches every disk
# without a filesystem. It must be refused, and nothing written.
EMPTY_RULE="${TMP_ROOT}/empty.rules"
if out=$(write_journal_udev_rule "$EMPTY_RULE" "" 2>&1); then
    bad "an empty UUID must be refused" "$out"
elif [[ -e "$EMPTY_RULE" ]]; then
    bad "a refused rule must not be written" "$(cat "$EMPTY_RULE")"
else
    ok "an empty UUID is refused and no rule written"
fi

# udev's own parser on the generated file, where this host has one that
# can check a file offline (systemd 254 and later).
write_journal_udev_rule "$RULE" "$UUID_A"
if ! udevadm verify --help >/dev/null 2>&1; then
    echo "  skipped — udevadm verify is not available here to parse the rule"
elif out=$(udevadm verify "$RULE" 2>&1); then
    ok "udevadm verify accepts the generated rule"
else
    bad "udevadm rejects the generated rule" "$out"
fi

echo ""
echo "=== server-setup.sh journal tuning read-back ==="

# ---------------------------------------------------------------------
# The read-back is what turns a rule that matches nothing into a failed
# setup rather than a journal on kernel defaults, so it gets the same
# treatment as verify_kernel_params: a fake sysfs queue directory per
# case.
# ---------------------------------------------------------------------

# A queue directory showing JOURNAL_QUEUE_TUNING as the kernel does once
# it is applied: `scheduler` lists every choice with the active one
# bracketed, the rest are a bare value. Prints the directory.
fake_queue() {
    local dir="${TMP_ROOT}/$1/queue" param key val
    mkdir -p "$dir"
    for param in "${JOURNAL_QUEUE_TUNING[@]}"; do
        key="${param% *}"
        val="${param#* }"
        if [[ "$key" == "scheduler" ]]; then
            printf '[%s] mq-deadline kyber bfq\n' "$val" > "$dir/$key"
        else
            printf '%s\n' "$val" > "$dir/$key"
        fi
    done
    printf '%s' "$dir"
}

QUEUE=$(fake_queue tuned)
if out=$(verify_journal_queue_tuning "$QUEUE" 2>&1); then
    ok "a tuned disk passes, the bracketed scheduler read as the active one"
else
    bad "every setting is in place, so the read-back must pass" "$out"
fi

# A disk the rule never reached: the kernel defaults, as the journal disk
# showed them on the hosts whose names swapped. Every difference must be
# named, and nothing that does match.
QUEUE=$(fake_queue untuned)
printf 'none [mq-deadline] kyber bfq\n' > "$QUEUE/scheduler"
printf '1023\n' > "$QUEUE/nr_requests"
if out=$(verify_journal_queue_tuning "$QUEUE" 2>&1 >/dev/null); then
    bad "an untuned disk must fail the read-back" "verify_journal_queue_tuning returned 0"
elif grep -q "did not tune the disk at $QUEUE" <<< "$out" \
    && grep -q "scheduler=mq-deadline" <<< "$out" \
    && grep -q "nr_requests=1023" <<< "$out" \
    && ! grep -q "nomerges" <<< "$out"; then
    ok "an untuned disk fails, naming exactly the settings that differ"
else
    bad "the failure should name the disk and each differing setting only" "$out"
fi

# An attribute this kernel does not offer cannot be set by any rule; it
# is reported, not counted against the disk.
QUEUE=$(fake_queue missing-attr)
rm "$QUEUE/add_random"
if out=$(verify_journal_queue_tuning "$QUEUE" 2>&1) \
    && grep -q "add_random: (not available)" <<< "$out"; then
    ok "an attribute the kernel lacks is reported and skipped"
else
    bad "a missing attribute should be reported, not fail the disk" "$out"
fi

echo ""
echo "=== server-setup.sh journal disk choice ==="

# ---------------------------------------------------------------------
# The hosts this was found on: an OS drive, a second drive of its size,
# and two identical larger ones, neither partitioned. Their kernel names
# swapped across the setup's own reboot, so a pick by name took the empty
# one on the re-run and the setup refused. `pick` runs pick_journal_disk
# on `drives` and prints its exit status, then what it printed.
# ---------------------------------------------------------------------
SAMSUNG=1920383410176
MICRON=960197124096
pick() {
    local drives="$1" got rc
    shift
    got=$(printf '%s' "$drives" | pick_journal_disk "$@" 2>/dev/null)
    rc=$?
    printf '%s %s' "$rc" "$got"
}
FIRST_BOOT="/dev/nvme0n1 $MICRON 1 182510B71596
/dev/nvme1n1 $MICRON 0 182510B74ABA
/dev/nvme2n1 $SAMSUNG 0 S7RHNG0YA05380
/dev/nvme3n1 $SAMSUNG 0 S7RHNG0YA05292
"
# The same drives after the reboot: the two Samsungs swapped names.
SWAPPED="/dev/nvme0n1 $MICRON 1 182510B71596
/dev/nvme1n1 $MICRON 0 182510B74ABA
/dev/nvme3n1 $SAMSUNG 0 S7RHNG0YA05380
/dev/nvme2n1 $SAMSUNG 0 S7RHNG0YA05292
"

got=$(pick "$FIRST_BOOT" "" "" "" "")
if [[ "$got" == "0 /dev/nvme3n1" ]]; then
    ok "a tie in size goes to the lowest serial, not the first name"
else
    bad "expected the Samsung with the lower serial, nvme3n1" "got: [${got}]"
fi
got=$(pick "$SWAPPED" "" "" "" "")
if [[ "$got" == "0 /dev/nvme2n1" ]]; then
    ok "the tie-break follows the drive when the names swap"
else
    bad "expected the same serial under its new name, nvme2n1" "got: [${got}]"
fi
got=$(pick "$(printf '%s' "$FIRST_BOOT" | tac)" "" "" "" "")
if [[ "$got" == "0 /dev/nvme3n1" ]]; then
    ok "the pick does not depend on the order the drives are listed in"
else
    bad "listing order changed the pick" "got: [${got}]"
fi

# A re-run keeps the journal it has, wherever its filesystem now is, even
# when the size ranking or the tie-break would choose another drive.
JOURNAL_UUID_NOW="ab33eea2-58ba-41c4-88f9-78513aef44e1"
got=$(pick "$SWAPPED" "" "" "$JOURNAL_UUID_NOW" "/dev/nvme3n1")
if [[ "$got" == "0 /dev/nvme3n1" ]]; then
    ok "an existing journal is kept over the drive the tie-break prefers"
else
    bad "the drive carrying fstab's filesystem must be kept" "got: [${got}]"
fi
got=$(pick "$FIRST_BOOT" "" "" "$JOURNAL_UUID_NOW" "/dev/nvme1n1")
if [[ "$got" == "0 /dev/nvme1n1" ]]; then
    ok "an existing journal is kept over a larger drive"
else
    bad "the drive carrying fstab's filesystem must be kept" "got: [${got}]"
fi

# A journal whose drive is gone must not be replaced by a fresh one.
if out=$(printf '%s' "$SWAPPED" | pick_journal_disk "" "" "$JOURNAL_UUID_NOW" "" 2>&1); then
    bad "a UUID no drive carries must be refused" "printed: [${out}]"
elif grep -q "which no drive" <<< "$out" && grep -q "remove that /etc/fstab line" <<< "$out" \
    && ! grep -q '^/dev/' <<< "$out"; then
    ok "a journal whose drive is gone is refused, not replaced"
else
    bad "the refusal should name the missing UUID and the way out" "$out"
fi

# JOURNAL_DISK wins on a fresh host, and on one whose journal it names.
got=$(pick "$FIRST_BOOT" "/dev/nvme1n1" "" "" "")
if [[ "$got" == "0 /dev/nvme1n1" ]]; then
    ok "JOURNAL_DISK is taken on a host with no journal yet"
else
    bad "JOURNAL_DISK must win over the size ranking" "got: [${got}]"
fi
got=$(pick "$SWAPPED" "/dev/nvme3n1" "$JOURNAL_UUID_NOW" "$JOURNAL_UUID_NOW" "/dev/nvme3n1")
if [[ "$got" == "0 /dev/nvme3n1" ]]; then
    ok "JOURNAL_DISK naming the existing journal is taken"
else
    bad "JOURNAL_DISK carrying fstab's filesystem must be accepted" "got: [${got}]"
fi
# Naming another drive than the journal fstab mounts would format it while
# the next boot mounts the old one.
if out=$(printf '%s' "$SWAPPED" \
    | pick_journal_disk "/dev/nvme2n1" "" "$JOURNAL_UUID_NOW" "/dev/nvme3n1" 2>&1); then
    bad "JOURNAL_DISK naming another drive than the journal must be refused" "printed: [${out}]"
elif grep -q "UUID=$JOURNAL_UUID_NOW (/dev/nvme3n1)" <<< "$out" \
    && grep -q "Re-run without JOURNAL_DISK" <<< "$out" \
    && ! grep -q '^/dev/' <<< "$out"; then
    ok "JOURNAL_DISK naming another drive than the journal is refused"
else
    bad "the refusal should name the journal's drive and both ways out" "$out"
fi

# Drives a serial cannot tell apart leave nothing to break the tie with.
got=$(pick "/dev/nvme2n1 $SAMSUNG 0
/dev/nvme3n1 $SAMSUNG 0
" "" "" "" "")
if [[ "$got" == "1 " ]]; then
    ok "two drives of one size with no serial are refused, not guessed between"
else
    bad "a tie the serial cannot break must be refused" "got: [${got}]"
fi
# Such a tie below the largest drive does not matter.
got=$(pick "/dev/nvme1n1 $MICRON 0
/dev/nvme2n1 $MICRON 0
/dev/nvme3n1 $SAMSUNG 0
" "" "" "" "")
if [[ "$got" == "0 /dev/nvme3n1" ]]; then
    ok "a tie between smaller drives does not block the largest"
else
    bad "only a tie for the largest drive is ambiguous" "got: [${got}]"
fi
# And a later, lower serial resolves a tie between two higher ones.
got=$(pick "/dev/nvme1n1 $SAMSUNG 0 S9
/dev/nvme2n1 $SAMSUNG 0 S9
/dev/nvme3n1 $SAMSUNG 0 S1
" "" "" "" "")
if [[ "$got" == "0 /dev/nvme3n1" ]]; then
    ok "a lower serial settles a tie between two drives above it"
else
    bad "the lowest serial is unambiguous" "got: [${got}]"
fi

# Partitioned drives hold the OS; with nothing else, there is no journal
# disk, which is not an error.
got=$(pick "/dev/nvme0n1 $SAMSUNG 1 S1
" "" "" "" "")
if [[ "$got" == "0 " ]]; then
    ok "a host with only partitioned drives has no journal disk, and no error"
else
    bad "expected an empty pick and success" "got: [${got}]"
fi

FSTAB="${TMP_ROOT}/fstab"
cat > "$FSTAB" << 'EOF'
# /mnt/journal was on /dev/nvme0n1 during installation
UUID=2f5ca87d-e6d1-43f7-acad-702148ea8a74 /               ext4    errors=remount-ro 0       1
UUID=A52E-1C69  /boot/efi       vfat    umask=0077      0       1
UUID=ab33eea2-58ba-41c4-88f9-78513aef44e1 /mnt/journal xfs noatime,logbsize=256k,logbufs=8 0 2
EOF
if got=$(fstab_journal_uuid "$FSTAB" /mnt/journal) && [[ "$got" == "$JOURNAL_UUID_NOW" ]]; then
    ok "fstab's journal UUID is read, comments mentioning the mount skipped"
else
    bad "expected the journal entry's UUID" "got: [${got}]"
fi
if got=$(fstab_journal_uuid "$FSTAB" /mnt/elsewhere) && [[ -z "$got" ]]; then
    ok "no fstab entry for the mount reads as no journal"
else
    bad "an absent entry must print nothing and succeed" "got: [${got}]"
fi
if got=$(fstab_journal_uuid "${TMP_ROOT}/no-fstab" /mnt/journal) && [[ -z "$got" ]]; then
    ok "a missing fstab reads as no journal"
else
    bad "a missing fstab must print nothing and succeed" "got: [${got}]"
fi
# A final line without its newline is still read.
printf 'UUID=%s /mnt/journal xfs noatime 0 2' "$JOURNAL_UUID_NOW" > "$FSTAB"
if got=$(fstab_journal_uuid "$FSTAB" /mnt/journal) && [[ "$got" == "$JOURNAL_UUID_NOW" ]]; then
    ok "an entry on a last line with no newline is read"
else
    bad "the last line must be read without its newline" "got: [${got}]"
fi
# An entry by kernel name says nothing once the names have moved.
printf '/dev/nvme2n1 /mnt/journal xfs noatime 0 2\n' > "$FSTAB"
if out=$(fstab_journal_uuid "$FSTAB" /mnt/journal 2>&1); then
    bad "an entry by kernel name must be refused" "printed: [${out}]"
elif grep -q "from /dev/nvme2n1, not by filesystem UUID" <<< "$out"; then
    ok "an fstab entry by kernel name is refused"
else
    bad "the refusal should name the entry's source" "$out"
fi

echo ""
echo "=== server-setup.sh kernel-param verification ==="

# ---------------------------------------------------------------------
# The regression that motivated this whole change: update-grub ran, but
# the params never reached the generated config. Must be a hard failure.
# ---------------------------------------------------------------------
KERNEL_PARAMS=("isolcpus=nohz,domain,1-5" "nosmt" "iommu=pt")
setup_case "quiet splash" "quiet splash" "silent-noop"
if out=$(verify_kernel_params 2>&1); then
    bad "a config missing every param must fail" "verify_kernel_params returned 0"
else
    # The closing line as well as the opening one: an error path that
    # dies partway through its own message (an unset global under
    # `set -u`, say) still returns non-zero and still prints enough for a
    # first-line grep to match.
    if grep -q "did not reach" <<< "$out" \
        && grep -q "looking configured" <<< "$out"; then
        ok "params absent from grub.cfg → hard failure"
    else
        bad "failure message should name the problem, in full" "$out"
    fi
fi
if [[ -f "$REBOOT_FLAG" ]]; then
    bad "a hard failure must not claim a reboot would fix it"
else
    ok "no reboot flag written on a hard failure"
fi

# ---------------------------------------------------------------------
# Partial application — the subtler version of the same bug.
# ---------------------------------------------------------------------
setup_case "isolcpus=nohz,domain,1-5 nosmt" "isolcpus=nohz,domain,1-5 nosmt" "partial"
if out=$(verify_kernel_params 2>&1); then
    bad "a config missing one param must still fail" "$out"
elif grep -q "iommu=pt" <<< "$out"; then
    ok "a single missing param is caught and named"
else
    bad "the missing param should be named" "$out"
fi

# ---------------------------------------------------------------------
# Staged but not booted — the legitimate "reboot required" state.
# ---------------------------------------------------------------------
setup_case "isolcpus=nohz,domain,1-5 nosmt iommu=pt" "quiet" "pending"
if out=$(verify_kernel_params 2>&1); then
    if grep -q "REBOOT REQUIRED" <<< "$out"; then
        ok "in grub.cfg but not live → reboot required"
    else
        bad "should announce a pending reboot" "$out"
    fi
else
    bad "a staged-but-not-live config is not an error" "$out"
fi
if [[ -f "$REBOOT_FLAG" ]]; then
    ok "reboot flag written when a reboot is genuinely needed"
else
    bad "reboot flag missing"
fi

# ---------------------------------------------------------------------
# Fully applied and booted — the steady state a re-run should report.
# ---------------------------------------------------------------------
setup_case "isolcpus=nohz,domain,1-5 nosmt iommu=pt" \
    "isolcpus=nohz,domain,1-5 nosmt iommu=pt" "active"
if out=$(verify_kernel_params 2>&1); then
    if grep -q "are active on the running kernel" <<< "$out"; then
        ok "fully applied → reported active"
    else
        bad "should report all params active" "$out"
    fi
else
    bad "a fully applied config must not fail" "$out"
fi
if [[ -f "$REBOOT_FLAG" ]]; then
    bad "no reboot should be requested when everything is live"
else
    ok "no reboot flag when everything is already active"
fi

# ---------------------------------------------------------------------
# Exact-token matching. A substring test would call `nosmt` live when
# only `nosmtfoo` is present, or match a param inside another's value —
# both would report a tuned host that is not tuned.
# ---------------------------------------------------------------------
KERNEL_PARAMS=("nosmt")
setup_case "nosmt" "nosmtfoo" "substring"
if out=$(verify_kernel_params 2>&1); then
    if grep -q "REBOOT REQUIRED" <<< "$out"; then
        ok "'nosmtfoo' on the cmdline does not satisfy 'nosmt'"
    else
        bad "substring must not count as a live param" "$out"
    fi
else
    bad "grub.cfg carries the param, so this is reboot-pending" "$out"
fi

# A trailing-substring value must not satisfy a shorter param either.
KERNEL_PARAMS=("iommu=pt")
setup_case "iommu=pt" "intel_iommu=pt" "prefix"
if out=$(verify_kernel_params 2>&1); then
    if grep -q "REBOOT REQUIRED" <<< "$out"; then
        ok "'intel_iommu=pt' does not satisfy 'iommu=pt'"
    else
        bad "prefix match must not count" "$out"
    fi
else
    bad "grub.cfg carries the param, so this is reboot-pending" "$out"
fi

# The same exactness is required on the grub.cfg side. Without it a
# `nosmtfoo` in the generated config would be read as `nosmt` having
# landed, turning a hard failure into an endless "reboot required".
KERNEL_PARAMS=("nosmt")
setup_case "nosmtfoo" "nosmtfoo" "cfg-substring"
if out=$(verify_kernel_params 2>&1); then
    bad "'nosmtfoo' in grub.cfg must not satisfy 'nosmt'" "$out"
elif grep -q "did not reach" <<< "$out"; then
    ok "'nosmtfoo' in grub.cfg does not satisfy 'nosmt'"
else
    bad "expected a hard failure naming the config" "$out"
fi

# Only kernel command lines count. A param appearing in a comment or a
# menu title is not a param the kernel will see.
KERNEL_PARAMS=("nosmt")
case_globals "non-linux-line"
printf '# a comment mentioning nosmt\nmenuentry "nosmt build" {\n\tlinux\t/boot/vmlinuz ro quiet\n}\n' > "$GRUB_CFG"
printf 'BOOT_IMAGE=/boot/vmlinuz ro quiet\n' > "$CMDLINE_SOURCE"
if out=$(verify_kernel_params 2>&1); then
    bad "a param only in a comment must not count" "$out"
elif grep -q "did not reach" <<< "$out"; then
    ok "params are only counted on kernel lines, not comments/titles"
else
    bad "expected a hard failure" "$out"
fi

# ---------------------------------------------------------------------
# Migration hazard: a host set up by the older script has the params in
# /etc/default/grub already, so the drop-in adds a second copy. With a
# stale core range that silently changes which cores get isolated.
# ---------------------------------------------------------------------
KERNEL_PARAMS=("isolcpus=nohz,domain,1-5")
setup_case "isolcpus=nohz,domain,1-10 isolcpus=nohz,domain,1-5" \
    "isolcpus=nohz,domain,1-10 isolcpus=nohz,domain,1-5" "dup-conflict"
if out=$(verify_kernel_params 2>&1); then
    bad "a param set twice with different values must fail" "$out"
elif grep -q "set more than once" <<< "$out" \
    && grep -q "1-10" <<< "$out" && grep -q "1-5" <<< "$out" \
    && grep -q "only place they belong" <<< "$out"; then
    ok "conflicting duplicate is caught and both values shown"
else
    bad "should name the conflict and both values" "$out"
fi

# The identical param appearing on several menu entries (normal +
# recovery) is how a correct grub.cfg always looks — not a conflict.
KERNEL_PARAMS=("isolcpus=nohz,domain,1-5")
case_globals "multi-entry"
{
    printf '\tlinux\t/boot/vmlinuz ro isolcpus=nohz,domain,1-5\n'
    printf '\tlinux\t/boot/vmlinuz ro single isolcpus=nohz,domain,1-5\n'
} > "$GRUB_CFG"
printf 'BOOT_IMAGE=/boot/vmlinuz ro isolcpus=nohz,domain,1-5\n' > "$CMDLINE_SOURCE"
if out=$(verify_kernel_params 2>&1); then
    ok "same value on several menu entries is not a conflict"
else
    bad "repeating one value across entries is normal, not an error" "$out"
fi

# ---------------------------------------------------------------------
# A reboot flag left behind by an earlier run must be cleared once the
# params are live. server-deploy.sh only removes it on the path where it
# reboots the host itself, so a hand-rebooted box with a disk-backed /tmp
# keeps it, and the closing summary would report a pending reboot that
# this very run just proved unnecessary.
# ---------------------------------------------------------------------
KERNEL_PARAMS=("nosmt")
setup_case "nosmt" "nosmt" "stale-flag"
touch "$REBOOT_FLAG"
if out=$(verify_kernel_params 2>&1); then
    if [[ -f "$REBOOT_FLAG" ]]; then
        bad "a stale reboot flag must be cleared once every param is live" "$out"
    else
        ok "stale reboot flag cleared when the params are already active"
    fi
else
    bad "a fully applied config must not fail" "$out"
fi

# ---------------------------------------------------------------------
# A grub.cfg at another path is not the same failure as params that did
# not land, and must not be reported as one — that message tells the
# operator to distrust a host whose tuning may be perfectly fine.
# ---------------------------------------------------------------------
KERNEL_PARAMS=("nosmt")
setup_case "nosmt" "nosmt" "no-grub-cfg"
rm -f "$GRUB_CFG"
if out=$(verify_kernel_params 2>&1); then
    bad "a missing grub.cfg must fail" "verify_kernel_params returned 0"
elif grep -q "does not exist" <<< "$out" \
    && ! grep -q "did not reach" <<< "$out"; then
    ok "a missing grub.cfg is named as such, not as unapplied params"
else
    bad "should report the missing config, not a failed application" "$out"
fi

echo ""
echo "=== ${PASS} passed, ${FAIL} failed ==="
[[ "$FAIL" -eq 0 ]]
