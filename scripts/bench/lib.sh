# The LAN suite's logic that needs no host: sourced by lan-bench-suite.sh
# and by test-lan-bench-suite.sh, which runs it against fixtures.

# `conf_value <conf text> <key>`: a value of /etc/melin-dpdk.conf, its
# surrounding double quotes stripped (multi-word values are quoted).
conf_value() {
    awk -F= -v key="$2" '$1 == key { sub(/^[^=]*=/, ""); gsub(/^"|"$/, ""); print; exit }' <<< "$1"
}

# `dpdk_conf_mode <conf text>`: how a host's NIC runs DPDK, as its
# /etc/melin-dpdk.conf says: `mlx5`, `l3` or `sriov`. A conf that names
# no mode, or no conf at all (empty text), is SR-IOV: the suite runs
# dpdk-setup.sh unattended on a host without one, and SR-IOV on a bond is
# the only setup that succeeds that way (mlx5 needs DPDK_IP).
dpdk_conf_mode() {
    local mode
    mode=$(conf_value "$1" DPDK_MODE)
    echo "${mode:-sriov}"
}

# The DPDK replication modes the suite runs, in the order `all` runs
# them. `port` mode (a NIC of replication's own) is the node's too, but
# not yet the suite's.
SUITE_DPDK_REPLICATION=(shared queue)

# `dpdk_replication_modes <DPDK_REPLICATION>`: the setting's modes, a
# comma-separated list of SUITE_DPDK_REPLICATION or `all`, each once;
# empty is `shared`, the replication a DPDK node runs without a flag.
# Prints them space-separated; on anything else, says why on stderr and
# fails.
dpdk_replication_modes() {
    local list="${1:-shared}" mode modes=()
    [[ "$list" == all ]] && list=$(IFS=,; echo "${SUITE_DPDK_REPLICATION[*]}")
    # A trailing empty field is dropped by `read`; one inside the list is
    # kept, and refused below as an unknown mode.
    IFS=, read -ra modes <<< "$list"
    local chosen=() known seen
    for mode in "${modes[@]}"; do
        if [[ "$mode" == port ]]; then
            echo "error: DPDK_REPLICATION mode 'port' is not supported by the suite yet: ${SUITE_DPDK_REPLICATION[*]}" >&2
            return 1
        fi
        # Compared whole, not as a substring of the list: `shared queue`
        # (a space for the comma) is one unknown mode, not two.
        known=
        for seen in "${SUITE_DPDK_REPLICATION[@]}"; do
            [[ "$mode" == "$seen" ]] && known=1
        done
        if [[ -z "$known" ]]; then
            echo "error: unknown DPDK_REPLICATION mode '${mode}': ${SUITE_DPDK_REPLICATION[*]}" >&2
            return 1
        fi
        for seen in "${chosen[@]}"; do
            if [[ "$mode" == "$seen" ]]; then
                # Its runs would overwrite the first pass's results.
                echo "error: DPDK_REPLICATION lists '${mode}' twice" >&2
                return 1
            fi
        done
        chosen+=("$mode")
    done
    echo "${chosen[*]}"
}

# What a DPDK primary logs once its replication listener is up, whichever
# thread serves it: the shared mode's line (the client poll thread) and
# the split modes' (the replication poll thread) both begin with it, so
# one wait holds in every mode. test-lan-bench-suite.sh checks the node's
# source for both.
DPDK_REPLICATION_READY="DPDK replication sender started"

# `refuse_queue_mode <host> <conf text>`: whether `host` can run DPDK
# replication on a queue of its own, which needs the NIC to steer
# replication to it (mlx5); IAVF, an SR-IOV VF's driver, cannot. Says why
# on stderr and fails for a host that cannot, judged by its
# /etc/melin-dpdk.conf (empty text: it has none).
refuse_queue_mode() {
    local host="$1" conf="$2"
    [[ "$(dpdk_conf_mode "$conf")" != sriov ]] && return 0
    if [[ -z "$conf" ]]; then
        echo "error: DPDK_REPLICATION=queue: ${host} has no /etc/melin-dpdk.conf, and the suite's unattended setup cannot configure queue mode (it sets up SR-IOV on a bond, whose VFs cannot steer replication to a queue of its own, and mlx5 needs DPDK_IP); run scripts/dpdk/dpdk-setup.sh with DPDK_IP there first" >&2
    else
        echo "error: DPDK_REPLICATION=queue: ${host} runs DPDK on SR-IOV (DPDK_MODE=sriov in /etc/melin-dpdk.conf), whose VFs cannot steer replication to a queue of its own; queue mode needs mlx5" >&2
    fi
    return 1
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

# The least a queue must move each way, in packets per sampled second, to
# count as carrying its traffic in judge_steering. Between the idle
# chatter a queue sees and the least a run makes it carry: idle, queue 1
# carries a heartbeat per replica every 5 s (only while no batch goes
# out) and the TCP acks for them, and queue 0 the subnet's ARP and stray
# frames, a few packets a second between them; under load, every request
# is replicated to two replicas and acknowledged by both on queue 1, and
# arrives and is answered on queue 0, so even one request in flight at a
# millisecond a round trip moves a thousand packets a second each way on
# each queue. An order of magnitude from either.
STEERING_MIN_PACKETS_PER_SECOND=100

# `judge_steering <DPDK replication mode> <samples file>`: the verdict on
# how the primary's NIC split a run's traffic across its queues, from
# samples of its per-queue packet counters, each sample a `# sample` line
# followed by the `melin_dpdk_queue_{rx,tx}_packets_total` lines of one
# scrape. Only queue mode steers: in any other mode there is nothing to
# judge, and it passes.
#
# The counters are cumulative per port and queue, and say nothing of
# which flow a packet was on, so the verdict is on volume: what each
# queue moved, summed over the primary's ports (one in queue mode, which
# runs on no bond, but the sum costs nothing), between the first and
# the last complete samples (those with every line the fullest has: a
# scrape cut short by its timeout is not used), against
# STEERING_MIN_PACKETS_PER_SECOND times the samples between them. Queue 1
# must have moved replication, both ways (out to the replicas, their acks
# in), and queue 0 the clients', both ways; a queue that moved less in a
# direction degrades the run, as does a primary with no queue 1 at all.
# A flow steered to the wrong queue while the right one carries the rest
# is left to its consequences, which the counters cannot see but the
# suite does: a replication segment on queue 0 is reset by the client
# stack and the replica's link drops (the health verdict), and a client
# segment on queue 1 is reset by the replication stack and the load
# generator's connection fails (the run fails).
#
# Prints the verdict; fails unless it is `ok` (or not judged, in a mode
# with nothing to judge). Fewer than two complete samples is unjudged.
judge_steering() {
    local mode="$1"
    if [[ "$mode" != queue ]]; then
        echo "not judged: ${mode} mode steers no traffic"
        return 0
    fi
    awk -v floor="$STEERING_MIN_PACKETS_PER_SECOND" '
        /^# sample/ { n++; next }
        n && $1 ~ /^melin_dpdk_queue_(rx|tx)_packets_total\{/ {
            dir = ($1 ~ /_rx_/) ? "in" : "out"
            queue = $1
            sub(/.*queue="/, "", queue)
            sub(/".*/, "", queue)
            moved[n, dir, queue] += $2
            lines[n]++
            if (queue == 1) has_replication_queue = 1
        }
        function delta(dir, queue) {
            return moved[last, dir, queue] - moved[first, dir, queue]
        }
        END {
            for (i = 1; i <= n; i++) if (lines[i] > fullest) fullest = lines[i]
            for (i = 1; i <= n; i++) {
                if (fullest && lines[i] == fullest) { if (!first) first = i; last = i }
            }
            if (first == last) { print "UNJUDGED: fewer than two samples of the queue counters"; exit 1 }
            if (!has_replication_queue) {
                print "DEGRADED: the primary has no queue 1: replication is not on a queue of its own"
                exit 1
            }
            seconds = last - first
            least = floor * seconds
            # %.0f, not %d: some awks print %d through a 32-bit int.
            summary = sprintf("queue 0 (clients) %.0f in, %.0f out; queue 1 (replication) %.0f in, %.0f out; over %d sampled seconds",
                delta("in", 0), delta("out", 0), delta("in", 1), delta("out", 1), seconds)
            if (delta("in", 1) < least || delta("out", 1) < least)
                reasons = reasons "; replication is not on queue 1"
            if (delta("in", 0) < least || delta("out", 0) < least)
                reasons = reasons "; client traffic is not on queue 0"
            if (reasons == "") { print "ok: " summary; exit 0 }
            printf "DEGRADED: %s%s (each needs %.0f packets each way, %d a second)\n",
                summary, reasons, least, floor
            exit 1
        }' "$2"
}
