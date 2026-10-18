#!/usr/bin/env bash
# Tests for the LAN suite's host-free logic (lib.sh): the health and
# snapshot verdicts that decide whether a run passed, the snapshot
# settings and disk guard, and the reading of a host's DPDK configuration
# and disk counters. The fixtures are real output: node log lines as the
# nodes log them (NO_COLOR, as the suite launches them, and coloured),
# `findmnt -nv -o SOURCE,FSTYPE`, `lsblk -rnso NAME,TYPE`, and
# /proc/diskstats. The rest of the suite drives hosts and is exercised by
# running it. Not run in CI: run it after changing lib.sh.
#
# Usage: ./scripts/bench/test-lan-bench-suite.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

TMP_ROOT=$(mktemp -d)
trap 'rm -rf "$TMP_ROOT"' EXIT

PASSED=0
FAILED=0

check() {
    local name="$1" expected="$2" actual="$3"
    if [[ "$expected" == "$actual" ]]; then
        echo "  ok — ${name}"
        PASSED=$((PASSED + 1))
    else
        echo "  FAIL — ${name}"
        echo "    expected: ${expected}"
        echo "    actual:   ${actual}"
        FAILED=$((FAILED + 1))
    fi
}

# `outcome <command...>`: the command's status and output, on one line.
outcome() {
    local out rc=0
    out=$("$@") || rc=$?
    echo "${rc} ${out}"
}

# `samples_file <samples...>`: a health samples file as sample_metrics
# writes it, from samples given one per argument, `;` between metric
# lines; prints its path.
samples_file() {
    local file="${TMP_ROOT}/samples" sample
    : > "$file"
    for sample in "$@"; do
        echo "# sample" >> "$file"
        tr ';' '\n' <<< "$sample" | sed '/^$/d' >> "$file"
    done
    echo "$file"
}

# `verdict <expected replicas> <samples...>`: judge_health's line and
# status on those samples.
verdict() {
    local expected="$1"
    shift
    outcome judge_health "$(samples_file "$@")" "$expected"
}

LIVE2='melin_replicas_connected 2;melin_replica_catching_up{slot="0"} 0;melin_replica_catching_up{slot="1"} 0;melin_replica_evictions_total 0'

echo "=== judge_health ==="
check "two live replicas throughout pass" \
    "0 ok: 3 samples, 0 evictions, 0 short of replicas, 0 catching up" \
    "$(verdict 2 "$LIVE2" "$LIVE2" "$LIVE2")"
check "a replica short in one sample degrades the run" \
    "1 DEGRADED: 2 samples, 0 evictions, 1 short of replicas, 0 catching up" \
    "$(verdict 2 "$LIVE2" 'melin_replicas_connected 1;melin_replica_evictions_total 0')"
check "a replica catching up in one sample degrades the run" \
    "1 DEGRADED: 2 samples, 0 evictions, 0 short of replicas, 1 catching up" \
    "$(verdict 2 "$LIVE2" 'melin_replicas_connected 2;melin_replica_catching_up{slot="1"} 1')"
check "an eviction degrades the run even once the replica is back" \
    "1 DEGRADED: 2 samples, 3 evictions, 0 short of replicas, 0 catching up" \
    "$(verdict 2 'melin_replicas_connected 2;melin_replica_evictions_total 3' "$LIVE2")"
check "a sample the endpoint did not answer is not judged" \
    "0 ok: 1 samples, 0 evictions, 0 short of replicas, 0 catching up" \
    "$(verdict 2 "" "$LIVE2")"
check "a run with nothing to judge is unjudged" \
    "1 UNJUDGED: no health samples" \
    "$(verdict 2 "" "")"

echo ""
echo "=== conf_value ==="
CONF='DPDK_IP=10.0.0.101
DPDK_PREFIX=24
DPDK_MAC=02:00:0a:00:00:65
DPDK_EAL_ARGS="-a 0000:c1:00.1 --huge-dir=/mnt/huge_2m"
VLAN_ID=2170'
check "a plain value" "10.0.0.101" "$(conf_value "$CONF" DPDK_IP)"
check "a MAC, colons and all" "02:00:0a:00:00:65" "$(conf_value "$CONF" DPDK_MAC)"
check "a quoted multi-word value, unquoted" \
    "-a 0000:c1:00.1 --huge-dir=/mnt/huge_2m" "$(conf_value "$CONF" DPDK_EAL_ARGS)"
check "a key absent is empty" "" "$(conf_value "$CONF" DPDK_GATEWAY)"
check "a key is matched whole, not by prefix" "" "$(conf_value "$CONF" DPDK)"

echo ""
echo "=== seq_window ==="
SEQ='melin_journal_sequence'
check "the first and last sampled sequences" \
    "0 100 1000" \
    "$(outcome seq_window "$(samples_file "$LIVE2;$SEQ 100" "$SEQ 400;$LIVE2" "$LIVE2;$SEQ 1000")")"
check "a sample the endpoint did not answer is skipped, first or last" \
    "0 100 1000" \
    "$(outcome seq_window "$(samples_file "" "$SEQ 100" "$SEQ 1000" "")")"
check "a run with no sequence sampled is unjudged" \
    "1 UNJUDGED: no journal sequence sampled" \
    "$(outcome seq_window "$(samples_file "" "$LIVE2" "")")"
check "a sequence that went backwards is unjudged" \
    "1 UNJUDGED: the journal sequence went backwards" \
    "$(outcome seq_window "$(samples_file "$SEQ 1000" "$SEQ 4")")"

echo ""
echo "=== judge_snapshots ==="
SNAP=/var/lib/melin-bench/snapshots/primary.snapshot
# Lines as a node logs them under the suite (NO_COLOR, timestamps, thread
# names and targets on), taken from a running echo-server.
saved() {
    local seq="$1" path="${2:-$SNAP}" ms="${3:-0.092626}" bytes="${4:-79}"
    echo "2026-10-18T19:45:35.254317Z  INFO       shadow melin_transport_core::shadow: shadow snapshot saved journal_seq=${seq} bytes=${bytes} elapsed_ms=${ms} attempts=1 path=${path}"
}
FAILED_LINE="2026-10-18T19:45:45.539959Z ERROR       shadow melin_transport_core::shadow: shadow snapshot failed journal_seq=9 error=snapshot I/O: Permission denied (os error 13) attempts=1 path=${SNAP}"
STARTED_LINE="2026-10-18T19:45:34.253955Z  INFO         main melin_server_runtime::server: shadow snapshot stage started interval_ms=10000 path=${SNAP}"
DEFERRED_LINE="2026-10-18T19:45:44.540011Z  WARN       shadow melin_transport_core::shadow: shadow snapshot deferred for a whole interval: the shadow has not reached a journal fsync boundary since it fell due; still retrying attempts=3 path=${SNAP}"
# The same saved line as a node logs it with colours on.
ANSI_LINE=$'\e[2m2026-10-18T19:45:44.540011Z\e[0m \e[32m INFO\e[0m       shadow \e[2mmelin_transport_core::shadow\e[0m\e[2m:\e[0m shadow snapshot saved \e[3mjournal_seq\e[0m\e[2m=\e[0m500 \e[3mbytes\e[0m\e[2m=\e[0m79 \e[3melapsed_ms\e[0m\e[2m=\e[0m0.085126 \e[3mattempts\e[0m\e[2m=\e[0m1 \e[3mpath\e[0m\e[2m=\e[0m'"$SNAP"

# `snapshots <first> <last> <measured ms> <interval ms> <path> <lines...>`:
# judge_snapshots's line and status on a log of those lines.
snapshots() {
    local file="${TMP_ROOT}/node.log"
    printf '%s\n' "${@:6}" > "$file"
    outcome judge_snapshots "$file" "$1" "$2" "$3" "$4" "$5"
}

check "enough saves in the window pass, with the slowest and the largest" \
    "0 ok: 3 saves in window (at least 2), 0 outside, 0 failed, 0 to another path, 0 unreadable; slowest 12.250 ms, largest 10000 bytes" \
    "$(snapshots 100 1000 30000 10000 "$SNAP" "$STARTED_LINE" \
        "$(saved 200 "$SNAP" 1.5 900)" "$(saved 500 "$SNAP" 12.25 79)" "$(saved 900 "$SNAP" 3 10000)")"
check "the slowest save's time, logged at full precision, is printed to the microsecond" \
    "0 ok: 1 saves in window (at least 1), 0 outside, 0 failed, 0 to another path, 0 unreadable; slowest 0.209 ms, largest 79 bytes" \
    "$(snapshots 100 1000 10000 10000 "$SNAP" "$(saved 500 "$SNAP" 0.20887799999999998)")"
check "no save in the window fails" \
    "1 NO SNAPSHOT: 0 saves in window (at least 2), 0 outside, 0 failed, 0 to another path, 0 unreadable; slowest - ms, largest - bytes" \
    "$(snapshots 100 1000 30000 10000 "$SNAP" "$STARTED_LINE" "$DEFERRED_LINE")"
check "fewer saves than expected fail" \
    "1 TOO FEW: 1 saves in window (at least 2), 0 outside, 0 failed, 0 to another path, 0 unreadable; slowest 0.093 ms, largest 79 bytes" \
    "$(snapshots 100 1000 30000 10000 "$SNAP" "$(saved 500)")"
check "idle saves before the load and cooldown saves after it are not counted" \
    "1 NO SNAPSHOT: 0 saves in window (at least 2), 3 outside, 0 failed, 0 to another path, 0 unreadable; slowest - ms, largest - bytes" \
    "$(snapshots 100 1000 30000 10000 "$SNAP" "$(saved 4)" "$(saved 8)" "$(saved 1200)")"
check "only the saves inside the window count" \
    "0 ok: 2 saves in window (at least 2), 3 outside, 0 failed, 0 to another path, 0 unreadable; slowest 0.093 ms, largest 79 bytes" \
    "$(snapshots 100 1000 30000 10000 "$SNAP" "$(saved 4)" "$(saved 8)" "$(saved 500)" "$(saved 600)" "$(saved 1200)")"
check "a save at exactly the first sequence is not counted, one at the last is" \
    "0 ok: 1 saves in window (at least 1), 1 outside, 0 failed, 0 to another path, 0 unreadable; slowest 0.093 ms, largest 79 bytes" \
    "$(snapshots 100 1000 10000 10000 "$SNAP" "$(saved 100)" "$(saved 1000)")"
check "a save at exactly the first sequence alone is no snapshot" \
    "1 NO SNAPSHOT: 0 saves in window (at least 1), 1 outside, 0 failed, 0 to another path, 0 unreadable; slowest - ms, largest - bytes" \
    "$(snapshots 100 1000 10000 10000 "$SNAP" "$(saved 100)")"
check "one failed save fails the run, however many landed" \
    "1 FAILED: 2 saves in window (at least 2), 0 outside, 1 failed, 0 to another path, 0 unreadable; slowest 0.093 ms, largest 79 bytes" \
    "$(snapshots 100 1000 30000 10000 "$SNAP" "$(saved 200)" "$FAILED_LINE" "$(saved 900)")"
check "a save to another path fails the run, however many landed" \
    "1 MISPLACED: 2 saves in window (at least 2), 0 outside, 0 failed, 1 to another path, 0 unreadable; slowest 0.093 ms, largest 79 bytes" \
    "$(snapshots 100 1000 30000 10000 "$SNAP" "$(saved 200)" "$(saved 300 /mnt/journal/primary.snapshot)" "$(saved 900)")"
check "a path that merely starts with the node's is another path" \
    "1 MISPLACED: 0 saves in window (at least 1), 0 outside, 0 failed, 1 to another path, 0 unreadable; slowest - ms, largest - bytes" \
    "$(snapshots 100 1000 10000 10000 "$SNAP" "$(saved 300 "${SNAP}.tmp")")"
check "a path with a space is matched whole" \
    "0 ok: 1 saves in window (at least 1), 0 outside, 0 failed, 0 to another path, 0 unreadable; slowest 0.093 ms, largest 79 bytes" \
    "$(snapshots 100 1000 10000 10000 "/srv/my snaps/primary.snapshot" "$(saved 300 "/srv/my snaps/primary.snapshot")")"
check "a saved line with no sequence fails the run" \
    "1 UNREADABLE: 1 saves in window (at least 1), 0 outside, 0 failed, 0 to another path, 1 unreadable; slowest 0.093 ms, largest 79 bytes" \
    "$(snapshots 100 1000 10000 10000 "$SNAP" "$(saved 300)" "INFO shadow snapshot saved bytes=79 path=${SNAP}")"
check "fields in another order, with colours, or with no timestamp still parse" \
    "0 ok: 3 saves in window (at least 2), 0 outside, 0 failed, 0 to another path, 0 unreadable; slowest 7.500 ms, largest 4096 bytes" \
    "$(snapshots 100 1000 30000 10000 "$SNAP" "$ANSI_LINE" \
        "2026-10-18T19:45:36.254275Z  INFO shadow snapshot saved path=${SNAP} attempts=2 elapsed_ms=7.5 bytes=4096 journal_seq=600" \
        "INFO melin_transport_core::shadow: shadow snapshot saved journal_seq=700 bytes=79 elapsed_ms=0.05 attempts=1 path=${SNAP}")"
check "the expected count follows the measured phase" \
    "1 TOO FEW: 4 saves in window (at least 5), 0 outside, 0 failed, 0 to another path, 0 unreadable; slowest 0.093 ms, largest 79 bytes" \
    "$(snapshots 100 1000 60000 10000 "$SNAP" "$(saved 200)" "$(saved 400)" "$(saved 600)" "$(saved 800)")"
check "a phase shorter than two intervals still expects one save" \
    "1 NO SNAPSHOT: 0 saves in window (at least 1), 0 outside, 0 failed, 0 to another path, 0 unreadable; slowest - ms, largest - bytes" \
    "$(snapshots 100 1000 5000 2000 "$SNAP" "$STARTED_LINE")"
check "an empty log is unjudged" \
    "1 UNJUDGED: no node log" \
    "$(: > "${TMP_ROOT}/empty.log"; outcome judge_snapshots "${TMP_ROOT}/empty.log" 100 1000 30000 10000 "$SNAP")"
check "a missing log is unjudged" \
    "1 UNJUDGED: no node log" \
    "$(outcome judge_snapshots "${TMP_ROOT}/absent.log" 100 1000 30000 10000 "$SNAP")"
check "interval 0 is off, and passes on any log" \
    "0 off" \
    "$(outcome judge_snapshots "${TMP_ROOT}/absent.log" 100 1000 30000 0 "$SNAP")"
check "interval 0 is off, whatever the window bounds" \
    "0 off" \
    "$(outcome judge_snapshots "${TMP_ROOT}/absent.log" "" "" "" 0 "$SNAP")"
check "a window bound that is not a count is unjudged" \
    "1 UNJUDGED: '' is not a count" \
    "$(snapshots "" 1000 30000 10000 "$SNAP" "$(saved 500)")"

echo ""
echo "=== mount_source ==="
check "a disk's filesystem gives its device" \
    "0 /dev/nvme0n1p2" "$(outcome mount_source "/dev/nvme0n1p2 ext4")"
check "a btrfs subvolume, as -v prints it, gives its device" \
    "0 /dev/nvme0n1p3" "$(outcome mount_source "/dev/nvme0n1p3 btrfs")"
check "tmpfs is refused" \
    "1 tmpfs: in memory, not on a disk" "$(outcome mount_source "shm    tmpfs")"
check "ramfs is refused" \
    "1 ramfs: in memory, not on a disk" "$(outcome mount_source "none ramfs")"
check "an overlay gives its source, for lsblk to leave unresolved" \
    "0 overlay" "$(outcome mount_source "overlay overlay")"
check "no output is unresolved" \
    "1 unresolved: no filesystem" "$(outcome mount_source "")"

echo ""
echo "=== shared_disks ==="
ROOT_PART=$'nvme0n1p2 part\nnvme0n1 disk'
JOURNAL_DISK='nvme1n1 disk'
check "two disks share nothing" \
    "0 " "$(outcome shared_disks "$ROOT_PART" "$JOURNAL_DISK")"
check "a partition against another on its own disk is shared" \
    "0 nvme0n1" "$(outcome shared_disks "$ROOT_PART" $'nvme0n1p3 part\nnvme0n1 disk')"
check "a partition against its whole disk is shared" \
    "0 nvme0n1" "$(outcome shared_disks "$ROOT_PART" 'nvme0n1 disk')"
check "an LV whose PVs span both disks is shared" \
    "0 sdb" "$(outcome shared_disks $'vg0-root lvm\nsda3 part\nsda disk\nsdb1 part\nsdb disk' $'sdb2 part\nsdb disk')"
check "an md array is shared with each of its members' disks" \
    "0 sda" "$(outcome shared_disks $'md0 raid1\nsda1 part\nsda disk\nsdb1 part\nsdb disk' $'sda2 part\nsda disk')"
check "two LVs on the same two PVs share both, each named once" \
    "0 sda sdb" "$(outcome shared_disks $'vg0-snap lvm\nsda3 part\nsda disk\nsdb1 part\nsdb disk' $'vg0-journal lvm\nsda3 part\nsda disk\nsdb1 part\nsdb disk')"
check "a disk listed twice on one side (md over two of its partitions) is named once" \
    "0 sda" "$(outcome shared_disks $'md0 raid1\nsda1 part\nsda disk\nsda2 part\nsda disk' $'sda3 part\nsda disk')"
check "a disk whose name is a prefix of another's is not shared with it" \
    "0 " "$(outcome shared_disks $'sda1 part\nsda disk' $'sdaa1 part\nsdaa disk')"
check "an empty (overlay) side is unresolved" \
    "1 unresolved" "$(outcome shared_disks "" "$JOURNAL_DISK")"
check "either side" \
    "1 unresolved" "$(outcome shared_disks "$ROOT_PART" "")"
check "a side with no disk among its devices is unresolved" \
    "1 unresolved" "$(outcome shared_disks 'loop0 loop' "$JOURNAL_DISK")"

echo ""
echo "=== duration_ms ==="
check "milliseconds" "0 500" "$(outcome duration_ms 500ms)"
check "seconds" "0 30000" "$(outcome duration_ms 30s)"
check "minutes" "0 120000" "$(outcome duration_ms 2m)"
check "a leading zero is decimal, not octal" "0 8000" "$(outcome duration_ms 08s)"
check "the largest duration that is still a count" "0 999999999960000" \
    "$(outcome duration_ms 16666666666m)"
# The last two fit the input's digit cap but not, in milliseconds, a count.
for refused in 1h30m 60 "" 1.5s -5s 30S " 30s" 1000000000000s 16666666667m 999999999999m; do
    check "'${refused}' is refused" \
        "1 '${refused}' is not a duration the suite can judge (<n>ms, <n>s or <n>m)" \
        "$(outcome duration_ms "$refused")"
done

echo ""
echo "=== snapshot_settings_check ==="
check "0 turns the shadow off" "0 " "$(outcome snapshot_settings_check 0 30000)"
check "below the floor is refused" \
    "1 SNAPSHOT_INTERVAL_MS=999 is below the 1000 ms floor" \
    "$(outcome snapshot_settings_check 999 30000)"
check "the floor itself is accepted" "0 " "$(outcome snapshot_settings_check 1000 30000)"
check "the shortest measured phase itself is accepted" "0 " "$(outcome snapshot_settings_check 30000 30000)"
check "above the shortest measured phase is refused" \
    "1 SNAPSHOT_INTERVAL_MS=30001 is longer than the shortest measured phase (30000 ms): no save would be judged" \
    "$(outcome snapshot_settings_check 30001 30000)"
check "a leading zero is decimal" "0 " "$(outcome snapshot_settings_check 09000 30000)"
check "a value that is not a number is refused" \
    "1 SNAPSHOT_INTERVAL_MS='10s' is not a whole number of milliseconds" \
    "$(outcome snapshot_settings_check 10s 30000)"
check "an empty value is refused" \
    "1 SNAPSHOT_INTERVAL_MS='' is not a whole number of milliseconds" \
    "$(outcome snapshot_settings_check "" 30000)"
check "a shortest phase that is not a number is refused" \
    "1 the shortest measured phase '' is not a whole number of milliseconds" \
    "$(outcome snapshot_settings_check 10000 "")"

echo ""
echo "=== extra_args_conflict ==="
check "--snapshot-interval-ms" "0 --snapshot-interval-ms" \
    "$(outcome extra_args_conflict "--cores none --snapshot-interval-ms 5000")"
check "--snapshot-path" "0 --snapshot-path" "$(outcome extra_args_conflict "--snapshot-path /x")"
check "--snapshot, which no longer exists" "0 --snapshot" "$(outcome extra_args_conflict "--snapshot /x")"
check "--snapshot=x" "0 --snapshot" "$(outcome extra_args_conflict "--snapshot=/x")"
check "--snapshot-path=x" "0 --snapshot-path" "$(outcome extra_args_conflict "--cores none --snapshot-path=/x")"
check "the first found is named" "0 --snapshot-path" \
    "$(outcome extra_args_conflict "--snapshot-path /x --snapshot-interval-ms 5000")"
check "a flag that merely shares a prefix is not matched" "1 " \
    "$(outcome extra_args_conflict "--snapshot-interval-msx 5000 --snapshots")"
check "a flag's name as another flag's value is still named" "0 --snapshot" \
    "$(outcome extra_args_conflict "--journal --snapshot")"
check "no arguments" "1 " "$(outcome extra_args_conflict "")"
check "no snapshot flag" "1 " "$(outcome extra_args_conflict "--cores none --max-journal-mib 0")"

echo ""
echo "=== diskstat_written ==="
# /proc/diskstats as the kernel prints it: a partition listed before
# would match a prefix test, so the device is matched whole.
DISKSTATS_BEFORE=' 259       1 nvme0n1p1 485 0 48832 55 0 0 0 0 0 52 55 0 0 0 0 0 0
 259       0 nvme0n1 23582612 4137763 2888204056 23064272 32717593 29766900 2368593488 81728235 0 1942875 104933278 2123605 0 51411351064 61793 365245 78976
 259       5 nvme1n1 884 348 113624 139 2 0 16 0 0 129 139 16 0 749520 0 0 0'
DISKSTATS_AFTER=' 259       1 nvme0n1p1 485 0 48832 55 7 0 9999 0 0 52 55 0 0 0 0 0 0
 259       0 nvme0n1 23582612 4137763 2888204056 23064272 32717600 29766900 2368597584 81728235 0 1942875 104933278 2123605 0 51411351064 61793 365245 78976
 259       5 nvme1n1 884 348 113624 139 2 0 16 0 0 129 139 16 0 749520 0 0 0'
check "a device present in both reads" \
    "0 2097152" "$(outcome diskstat_written "$DISKSTATS_BEFORE" "$DISKSTATS_AFTER" nvme0n1)"
check "a device with nothing written" \
    "0 0" "$(outcome diskstat_written "$DISKSTATS_BEFORE" "$DISKSTATS_AFTER" nvme1n1)"
check "a device missing from the second read fails rather than report 0" \
    "1 no nvme1n1 in /proc/diskstats" \
    "$(outcome diskstat_written "$DISKSTATS_BEFORE" "$(head -2 <<< "$DISKSTATS_AFTER")" nvme1n1)"
check "a device missing from the first read fails" \
    "1 no sdz in /proc/diskstats" "$(outcome diskstat_written "$DISKSTATS_BEFORE" "$DISKSTATS_AFTER" sdz)"
check "an empty read fails" \
    "1 no nvme0n1 in /proc/diskstats" "$(outcome diskstat_written "" "$DISKSTATS_AFTER" nvme0n1)"
check "a count that went backwards fails" \
    "1 the sectors written to nvme0n1 went backwards" \
    "$(outcome diskstat_written "$DISKSTATS_AFTER" "$DISKSTATS_BEFORE" nvme0n1)"


echo ""
echo "=== ${PASSED} passed, ${FAILED} failed ==="
[[ "$FAILED" -eq 0 ]]
