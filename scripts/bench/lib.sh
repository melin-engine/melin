# The LAN suite's logic that needs no host: sourced by lan-bench-suite.sh
# and by test-lan-bench-suite.sh, which runs it against fixtures.

# `conf_value <conf text> <key>`: a value of /etc/melin-dpdk.conf, its
# surrounding double quotes stripped (multi-word values are quoted).
conf_value() {
    awk -F= -v key="$2" '$1 == key { sub(/^[^=]*=/, ""); gsub(/^"|"$/, ""); print; exit }' <<< "$1"
}

# `judge_health <samples file> <expected replicas>`: the verdict on a
# run's health samples, each a `# sample` line followed by the primary's
# replica metrics. A replica evicted, missing or catching up in any
# sample degrades the run; a run with no sample to judge is unjudged.
# Prints the verdict; fails unless it is `ok`.
judge_health() {
    awk -v expected="$2" '
        function tally() {
            if (connected < 0) { missing++; return }
            if (connected < expected) short++
            if (catching) behind++
        }
        /^# sample/ { if (n++) tally(); connected = -1; catching = 0; next }
        $1 == "melin_replicas_connected" { connected = $2 }
        $1 ~ /^melin_replica_catching_up\{/ && $2 != 0 { catching = 1 }
        $1 == "melin_replica_evictions_total" && $2 > evictions { evictions = $2 }
        END {
            if (n) tally()
            judged = n - missing
            if (judged == 0) { print "UNJUDGED: no health samples"; exit 1 }
            verdict = (evictions + short + behind > 0) ? "DEGRADED" : "ok"
            printf "%s: %d samples, %d evictions, %d short of replicas, %d catching up\n",
                verdict, judged, evictions, short, behind
            exit verdict != "ok"
        }' "$1"
}

# Every helper below prints its result, or on failure the reason, on
# stdout, so that a caller under `set -e` reads either from one capture:
# `if ! ms=$(duration_ms "$d"); then echo "... ${ms}"; fi`.

# `is_count <value>`: whether `value` is a whole number of at most 15
# digits. The cap keeps a count exact in awk (a double holds every
# integer below 2^53) and its product with 512 (a sector count in bytes)
# inside bash's signed 64-bit arithmetic, which would otherwise wrap
# without a word.
is_count() {
    [[ "$1" =~ ^[0-9]{1,15}$ ]]
}

# `duration_ms <duration>`: a load phase's duration, as the suite's
# defaults spell it (`<n>ms`, `<n>s` or `<n>m`), in milliseconds. Any
# other form fails, even one the load generator would accept (`1h30m`):
# the suite refuses a duration it cannot judge rather than guess at it.
# The result must itself be a count (see is_count), as every helper that
# takes it requires; the 12-digit cap on the input only keeps the
# multiplication inside bash's 64-bit arithmetic.
duration_ms() {
    local n unit ms=""
    if [[ "$1" =~ ^([0-9]{1,12})(ms|s|m)$ ]]; then
        # Base 10 explicitly: bash reads a leading zero as octal.
        n=$((10#${BASH_REMATCH[1]})) unit="${BASH_REMATCH[2]}"
        case "$unit" in
            ms) ms="$n" ;;
            s) ms=$((n * 1000)) ;;
            m) ms=$((n * 60000)) ;;
        esac
    fi
    if ! is_count "$ms"; then
        echo "'$1' is not a duration the suite can judge (<n>ms, <n>s or <n>m)"
        return 1
    fi
    echo "$ms"
}

# `snapshot_settings_check <interval ms> <shortest measured phase ms>`:
# whether SNAPSHOT_INTERVAL_MS can be judged. `0` (the shadow off) always
# can. Otherwise the interval must be at least a second (back-to-back saves
# of a large state measure a configuration nobody runs) and at most the
# shortest measured phase (a longer one could land no save in it).
snapshot_settings_check() {
    local interval="$1" shortest="$2" floor=1000
    if ! is_count "$interval"; then
        echo "SNAPSHOT_INTERVAL_MS='${interval}' is not a whole number of milliseconds"
        return 1
    fi
    if ! is_count "$shortest"; then
        echo "the shortest measured phase '${shortest}' is not a whole number of milliseconds"
        return 1
    fi
    interval=$((10#$interval)) shortest=$((10#$shortest))
    if (( interval == 0 )); then
        return 0
    elif (( interval < floor )); then
        echo "SNAPSHOT_INTERVAL_MS=${interval} is below the ${floor} ms floor"
        return 1
    elif (( interval > shortest )); then
        echo "SNAPSHOT_INTERVAL_MS=${interval} is longer than the shortest measured phase (${shortest} ms): no save would be judged"
        return 1
    fi
}

# `extra_args_conflict <args>`: the first snapshot flag in a node's extra
# arguments, which the suite sets itself, alone or as `--flag=value`.
# Succeeds (a conflict found) after printing it; fails when there is none.
# `--snapshot` no longer exists, and is listed so that a stale setting is
# named rather than refused by the node with no log line to show for it.
extra_args_conflict() {
    local -a words
    local word flag
    read -ra words <<< "$1"
    for word in "${words[@]}"; do
        for flag in --snapshot-interval-ms --snapshot-path --snapshot; do
            if [[ "$word" == "$flag" || "$word" == "${flag}="* ]]; then
                echo "$flag"
                return 0
            fi
        done
    done
    return 1
}

# `mount_source <findmnt -nv -o SOURCE,FSTYPE output>`: the device a
# directory's filesystem is on, for `lsblk`. Fails on a filesystem held
# in memory (tmpfs, ramfs), whose writes never reach a disk, and on no
# output at all.
mount_source() {
    local device fstype
    read -r device fstype _ <<< "$1"
    case "$fstype" in
        "") echo "unresolved: no filesystem"; return 1 ;;
        tmpfs | ramfs) echo "${fstype}: in memory, not on a disk"; return 1 ;;
        *) echo "$device" ;;
    esac
}

# `shared_disks <lsblk -rnso NAME,TYPE A> <... B>`: the whole disks under
# both devices, which `lsblk -s` lists as each device's ancestors (through
# partitions, LVM, md), printed on one line; empty when the two share
# none. Fails with `unresolved` when either side has no disk at all (an
# overlay filesystem, a device lsblk does not know): a separation that
# cannot be shown is not assumed.
shared_disks() {
    # Space-delimited strings, not associative arrays: a device sits on a
    # handful of disks, so a whole-word match is as good as a set. Disk
    # names hold no spaces.
    local name type under_a=" " shared=" " under_b=0
    while read -r name type _; do
        if [[ "$type" == disk ]]; then under_a+="${name} "; fi
    done <<< "$1"
    while read -r name type _; do
        [[ "$type" == disk ]] || continue
        under_b=1
        if [[ "$under_a" == *" ${name} "* && "$shared" != *" ${name} "* ]]; then
            shared+="${name} "
        fi
    done <<< "$2"
    if [[ "$under_a" == " " ]] || (( under_b == 0 )); then
        echo "unresolved"
        return 1
    fi
    shared="${shared# }"
    echo "${shared% }"
}

# `seq_window <samples file>`: the first and last `melin_journal_sequence`
# in a run's health samples (see judge_health), the journal sequences
# that bound the load. A sample the endpoint did not answer has none and
# is skipped; a run with none at all is unjudged, as is one whose sequence
# went backwards (a node that restarted mid-run).
seq_window() {
    awk '
        $1 == "melin_journal_sequence" && $2 ~ /^[0-9]+$/ {
            if (!n++) first = $2
            if ($2 + 0 < last + 0) backwards = 1
            last = $2
        }
        END {
            if (!n) { print "UNJUDGED: no journal sequence sampled"; exit 1 }
            if (backwards) { print "UNJUDGED: the journal sequence went backwards"; exit 1 }
            print first, last
        }' "$1"
}

# `judge_snapshots <log> <first> <last> <measured ms> <interval ms> <path>`:
# the verdict on one node's shadow snapshots during a run, from its log.
# A save counts when its `journal_seq` lies in `first < seq <= last` (see
# seq_window; a save taken idle just before the load can carry exactly
# `first`, and must not count) and it went to `path`. At least
# `max(1, measured / interval - 1)` must count: the measured phase, not
# the whole load, so that where the saves fall against it does not
# matter. Any failed save, or a save to another path, fails the run
# however many counted. Interval `0` (the shadow off) is `off`, and
# passes, whatever the other arguments. Prints the verdict, with the
# slowest counted save's time (to the microsecond: the node logs a float
# at full precision) and the largest one's size; fails unless it is `ok`
# or `off`.
#
# The lines are tracing's: `... shadow snapshot saved journal_seq=4
# bytes=79 elapsed_ms=0.092626 attempts=1 path=/x/primary.snapshot`, the
# fields matched by name in any order, ANSI colours stripped. The path is
# matched whole and may hold spaces; the other values are single words.
judge_snapshots() {
    local log="$1" first="$2" last="$3" measured="$4" interval="$5" path="$6"
    # Off first, so that it passes whatever bounds come with it: with the
    # shadow off a run may have no journal window to give.
    if is_count "$interval" && (( 10#$interval == 0 )); then
        echo "off"
        return 0
    fi
    local arg
    for arg in "$first" "$last" "$measured" "$interval"; do
        if ! is_count "$arg"; then
            echo "UNJUDGED: '${arg}' is not a count"
            return 1
        fi
    done
    if [[ ! -s "$log" ]]; then
        echo "UNJUDGED: no node log"
        return 1
    fi
    local expected=$(( 10#$measured / 10#$interval - 1 ))
    (( expected >= 1 )) || expected=1
    # The path through the environment: `awk -v` would read its
    # backslashes as escapes.
    SNAPSHOT_PATH="$path" awk -v first="$first" -v last="$last" -v expected="$expected" '
        BEGIN { path = ENVIRON["SNAPSHOT_PATH"] }
        # The value of field `key`: the word after " key=", or empty.
        function field(line, key) {
            if (!match(line, " " key "=[^ ]*")) return ""
            return substr(line, RSTART + length(key) + 2, RLENGTH - length(key) - 2)
        }
        { gsub(/\033\[[0-9;]*m/, ""); line = " " $0 " " }
        line ~ / shadow snapshot failed / { failed++; next }
        line !~ / shadow snapshot saved / { next }
        {
            seq = field(line, "journal_seq")
            if (seq !~ /^[0-9]+$/) { unreadable++; next }
            if (!index(line, " path=" path " ")) { elsewhere++; next }
            if (seq + 0 <= first + 0 || seq + 0 > last + 0) { outside++; next }
            counted++
            ms = field(line, "elapsed_ms"); bytes = field(line, "bytes")
            if (slowest == "" || ms + 0 > slowest + 0) slowest = ms
            if (largest == "" || bytes + 0 > largest + 0) largest = bytes
        }
        END {
            if (failed) verdict = "FAILED"
            else if (elsewhere) verdict = "MISPLACED"
            else if (unreadable) verdict = "UNREADABLE"
            else if (!counted) verdict = "NO SNAPSHOT"
            else if (counted < expected) verdict = "TOO FEW"
            else verdict = "ok"
            printf "%s: %d saves in window (at least %d), %d outside, %d failed, %d to another path, %d unreadable; slowest %s ms, largest %s bytes\n",
                verdict, counted, expected, outside, failed, elsewhere, unreadable,
                slowest == "" ? "-" : sprintf("%.3f", slowest), largest == "" ? "-" : largest
            exit verdict != "ok"
        }' "$log"
}

# `diskstat_sectors <diskstats text> <device>`: the sectors written to
# `device` (the 10th field of /proc/diskstats), or a failure when the
# device is not listed.
diskstat_sectors() {
    awk -v dev="$2" '
        $3 == dev && $10 ~ /^[0-9]+$/ { print $10; found = 1; exit }
        END { if (!found) { print "no " dev " in /proc/diskstats"; exit 1 } }' <<< "$1"
}

# `diskstat_written <diskstats before> <diskstats after> <device>`: the
# bytes written to `device` between two reads of /proc/diskstats, whose
# sectors are 512 bytes whatever the device's own sector size. Fails when
# either read lacks the device (never reported as 0 bytes), or when the
# count went backwards (the host rebooted in between).
diskstat_written() {
    local before after
    before=$(diskstat_sectors "$1" "$3") || { echo "$before"; return 1; }
    after=$(diskstat_sectors "$2" "$3") || { echo "$after"; return 1; }
    if ! is_count "$before" || ! is_count "$after"; then
        echo "a sector count of ${3} is out of range"
        return 1
    fi
    if (( 10#$after < 10#$before )); then
        echo "the sectors written to ${3} went backwards"
        return 1
    fi
    echo $(( (10#$after - 10#$before) * 512 ))
}
