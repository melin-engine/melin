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
