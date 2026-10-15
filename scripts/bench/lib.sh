# The LAN suite's logic that needs no host: sourced by lan-bench-suite.sh,
# by its podman rehearsal test-containers.sh, by test-dpdk-setup.sh, and
# by test-lan-bench-suite.sh, which runs it against fixtures.
# dpdk-setup.sh does not source it (it ships to hosts standalone): its
# ipv4_to_int is mirrored here as ipv4_as_int, and test-dpdk-setup.sh
# checks the two agree.

# `conf_value <conf text> <key>`: a value of /etc/melin-dpdk.conf, its
# surrounding double quotes stripped (multi-word values are quoted).
conf_value() {
    awk -F= -v key="$2" '$1 == key { sub(/^[^=]*=/, ""); gsub(/^"|"$/, ""); print; exit }' <<< "$1"
}

# `dpdk_conf_mode <conf text>`: how a host's NIC runs DPDK, as its
# /etc/melin-dpdk.conf says: `mlx5`, `l3` or `sriov` (dpdk-setup.sh's),
# or `af_packet` (the container rig's, test-containers.sh, on its
# containers' veths). A conf that names no mode, or no conf at all (empty
# text), is SR-IOV: the suite runs dpdk-setup.sh unattended on a host
# without one, and SR-IOV on a bond is the only setup that succeeds that
# way (mlx5 needs DPDK_IP).
dpdk_conf_mode() {
    local mode
    mode=$(conf_value "$1" DPDK_MODE)
    echo "${mode:-sriov}"
}

# The DPDK replication modes the suite runs, in the order `all` runs
# them: every mode, so `all` asks every node host for what each needs
# (an mlx5 NIC for queue mode, a second one for port mode) and is refused
# up front, naming the host, where one lacks it, rather than quietly
# running fewer modes than asked for.
SUITE_DPDK_REPLICATION=(shared queue port)

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
# the split modes' (queue and port: the replication poll thread) both
# begin with it, so
# one wait holds in every mode. test-lan-bench-suite.sh checks the node's
# source for both.
DPDK_REPLICATION_READY="DPDK replication sender started"

# Why af_packet cannot run queue mode: refuse_queue_mode's reason for an
# af_packet host and test-containers.sh's for its DPDK=1 rig, one wording.
AF_PACKET_NO_QUEUE_MODE="which has one queue and no rte_flow to steer replication to a queue of its own; queue mode needs mlx5"

# `refuse_queue_mode <host> <conf text>`: whether `host` can run DPDK
# replication on a queue of its own, which needs the NIC to steer
# replication to it (mlx5); IAVF, an SR-IOV VF's driver, cannot, nor can
# af_packet, which has one queue and no rte_flow. Says why on stderr and
# fails for a host that cannot, judged by its /etc/melin-dpdk.conf (empty
# text: it has none).
refuse_queue_mode() {
    local host="$1" conf="$2" mode
    mode=$(dpdk_conf_mode "$conf")
    if [[ "$mode" == af_packet ]]; then
        echo "error: DPDK_REPLICATION=queue: ${host} runs DPDK on af_packet (DPDK_MODE=af_packet in /etc/melin-dpdk.conf), ${AF_PACKET_NO_QUEUE_MODE}" >&2
        return 1
    fi
    [[ "$mode" != sriov ]] && return 0
    if [[ -z "$conf" ]]; then
        echo "error: DPDK_REPLICATION=queue: ${host} has no /etc/melin-dpdk.conf, and the suite's unattended setup cannot configure queue mode (it sets up SR-IOV on a bond, whose VFs cannot steer replication to a queue of its own, and mlx5 needs DPDK_IP); run scripts/dpdk/dpdk-setup.sh with DPDK_IP there first" >&2
    else
        echo "error: DPDK_REPLICATION=queue: ${host} runs DPDK on SR-IOV (DPDK_MODE=sriov in /etc/melin-dpdk.conf), whose VFs cannot steer replication to a queue of its own; queue mode needs mlx5" >&2
    fi
    return 1
}

# The /etc/melin-dpdk.conf keys of a replication NIC: its DPDK port id,
# address and MAC, and one of DPDK_REPL_DEVICE_KEYS naming its device.
# dpdk-setup.sh writes them when given one (DPDK_REPL_NIC and
# DPDK_REPL_IP, mlx5 only), and the container rig (test-containers.sh)
# on its node containers.
DPDK_REPL_CONF_KEYS=(DPDK_REPL_PORT DPDK_REPL_IP DPDK_REPL_MAC)
# How a replication NIC's device is named, one or the other: a PCI
# address (a real port, dpdk-setup.sh's), which DPDK_EAL_ARGS allows with
# `-a <pci>`, or a virtual device's name (af_packet, the container
# rig's), which DPDK_EAL_ARGS creates with `--vdev=<name>[,<args>]`.
DPDK_REPL_DEVICE_KEYS=(DPDK_REPL_PCI DPDK_REPL_VDEV)

# `refuse_port_mode <host> <conf text>`: whether `host` can run DPDK
# replication on a NIC of its own, which needs one: every
# DPDK_REPL_CONF_KEYS key in its /etc/melin-dpdk.conf (empty text: it has
# none) and one DPDK_REPL_DEVICE_KEYS key, and DPDK_EAL_ARGS bringing up
# the replication NIC's device beside the client NIC's (the node's one
# EAL brings up both). A conf naming neither device is reported missing
# DPDK_REPL_PCI, as a real host's is. Says why on stderr and fails for a
# host that cannot, with the remedy for the kind of host it is: an
# af_packet one (DPDK_MODE=af_packet, or a virtual device named) has its
# conf written by the container rig, any other by dpdk-setup.sh. The rest
# (another port and address than the client NIC's, in its subnet) the
# node checks itself, before it touches a NIC, and its log says so.
refuse_port_mode() {
    local host="$1" conf="$2" key value named=() missing=() device_key device
    local setup="run scripts/dpdk/dpdk-setup.sh with DPDK_IP, DPDK_REPL_NIC and DPDK_REPL_IP there (a second mlx5 port, see docs/benchmarking.md)"
    if [[ -z "$conf" ]]; then
        echo "error: DPDK_REPLICATION=port: ${host} has no /etc/melin-dpdk.conf, so no NIC of replication's own; ${setup}" >&2
        return 1
    fi
    device_key="${DPDK_REPL_DEVICE_KEYS[0]}"
    for key in "${DPDK_REPL_DEVICE_KEYS[@]}"; do
        value=$(conf_value "$conf" "$key")
        [[ -n "$value" ]] && named+=("${key}=${value}") && device_key="$key"
    done
    if (( ${#named[@]} > 1 )); then
        local joined
        joined=$(printf ', %s' "${named[@]}")
        echo "error: DPDK_REPLICATION=port: ${host}'s /etc/melin-dpdk.conf names the replication NIC's device twice (${joined:2}); keep the one DPDK_EAL_ARGS brings up" >&2
        return 1
    fi
    if [[ "$device_key" == DPDK_REPL_VDEV || "$(conf_value "$conf" DPDK_MODE)" == af_packet ]]; then
        setup="an af_packet conf is the container rig's (DPDK=1 scripts/bench/test-containers.sh), which gives each node container a replication NIC on melin-bench-repl (see docs/benchmarking.md)"
    fi
    for key in "${DPDK_REPL_CONF_KEYS[@]}" "$device_key"; do
        [[ -z "$(conf_value "$conf" "$key")" ]] && missing+=("$key")
    done
    if (( ${#missing[@]} > 0 )); then
        echo "error: DPDK_REPLICATION=port: ${host}'s /etc/melin-dpdk.conf has no ${missing[*]}, so no NIC of replication's own; ${setup}" >&2
        return 1
    fi
    device=$(conf_value "$conf" "$device_key")
    local eal=" $(conf_value "$conf" DPDK_EAL_ARGS) "
    case "$device_key" in
        DPDK_REPL_VDEV)
            # Created bare or with devargs, in either of EAL's spellings
            # (`--vdev=<name>[,...]`, `--vdev <name>[,...]`).
            if [[ "$eal" != *" --vdev=${device} "* && "$eal" != *" --vdev=${device},"* \
                && "$eal" != *" --vdev ${device} "* && "$eal" != *" --vdev ${device},"* ]]; then
                echo "error: DPDK_REPLICATION=port: ${host}'s /etc/melin-dpdk.conf has DPDK_EAL_ARGS not creating the replication NIC's device (--vdev=${device}); add it there beside the client NIC's" >&2
                return 1
            fi
            ;;
        DPDK_REPL_PCI)
            # Allowed bare or with devargs (`-a <pci>,<key>=<value>`).
            if [[ "$eal" != *" -a ${device} "* && "$eal" != *" -a ${device},"* ]]; then
                echo "error: DPDK_REPLICATION=port: ${host}'s /etc/melin-dpdk.conf has DPDK_EAL_ARGS not allowing the replication NIC's device (-a ${device}); ${setup}" >&2
                return 1
            fi
            ;;
    esac
}

# `replication_flags <DPDK replication mode> <replication ports>
# <replication IP> [<primary's replication MAC>]`: what a DPDK node adds
# to its command line to run replication where the mode says, each flag
# after a space; nothing in shared mode. The ports, IP and MAC are port
# mode's (the node's replication NIC, and on a replica the primary's
# replication NIC's MAC, which its own dials), and are ignored elsewhere.
replication_flags() {
    local mode="$1" ports="$2" ip="$3" peer_mac="${4:-}"
    case "$mode" in
        queue) printf ' --dpdk-replication-queue' ;;
        port)
            printf ' --dpdk-replication-ports %s --dpdk-replication-ip %s' "$ports" "$ip"
            [[ -n "$peer_mac" ]] && printf ' --dpdk-replication-peer-mac %s' "$peer_mac"
            ;;
    esac
    return 0
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

# The least replication's queue or port, and the clients', must each move
# each way, in packets per sampled second, to count as carrying its
# traffic in judge_steering. Between the idle chatter each sees and the
# least a run makes it carry: idle, replication's carries a heartbeat per
# replica every 5 s (only while no batch goes out) and the TCP acks for
# them, and the clients' the subnet's ARP and stray frames, a few packets
# a second between them (a NIC of its own, in port mode, gets its share
# of the ARP too); under load, every request is replicated to two
# replicas and acknowledged by both on replication's, and arrives and is
# answered on the clients', so even one request in flight at a
# millisecond a round trip moves a thousand packets a second each way on
# each. An order of magnitude from either, in either mode.
STEERING_MIN_PACKETS_PER_SECOND=100

# `judge_steering <DPDK replication mode> <samples file> [<client ports>
# <replication ports>]`: the verdict on how the primary split a run's
# traffic between replication and its clients, from samples of its
# per-queue packet counters, each sample a `# sample` line followed by
# the `melin_dpdk_queue_{rx,tx}_packets_total` lines of one scrape,
# labelled by DPDK port and queue. In queue mode replication is queue 1
# of the client NIC's ports, the clients queue 0; in port mode
# replication is the replication NIC's ports (the third and fourth
# arguments, comma-separated DPDK port ids, as the node's flags take
# them; needed in port mode only), the clients the client NIC's, every
# queue of each. Shared mode puts both on one queue: there is nothing to
# judge, and it passes.
#
# The counters are cumulative per port and queue, and say nothing of
# which flow a packet was on, so the verdict is on volume: what each side
# moved, summed over its queues and ports (one port each on mlx5, which
# runs no bond, but the sum costs nothing), between the first and the
# last complete samples (those with every line the fullest has: a scrape
# cut short by its timeout is not used), against
# STEERING_MIN_PACKETS_PER_SECOND times the samples between them.
# Replication's queue or port must have moved replication, both ways (out
# to the replicas, their acks in), and the clients' theirs, both ways;
# one that moved less in a direction degrades the run, as does a primary
# with no replication queue or port at all. A flow on the wrong side
# while the right one carries the rest is left to its consequences, which
# the counters cannot see but the suite does: in queue mode a replication
# segment on queue 0 is reset by the client stack and the replica's link
# drops (the health verdict), and a client segment on queue 1 is reset by
# the replication stack and the load generator's connection fails (the
# run fails); in port mode each NIC answers for its own address only, so
# a flow on the wrong one is one sent to the wrong address, and fails the
# same ways.
#
# Prints the verdict; fails unless it is `ok` (or not judged, in a mode
# with nothing to judge). Fewer than two complete samples is unjudged, as
# is port mode without the ports to tell the sides apart.
judge_steering() {
    local mode="$1" client_ports="${3:-}" replication_ports="${4:-}"
    case "$mode" in
        queue) ;;
        port)
            if [[ -z "$client_ports" || -z "$replication_ports" ]]; then
                echo "UNJUDGED: port mode needs the client and replication NICs' DPDK ports"
                return 1
            fi
            ;;
        *)
            echo "not judged: ${mode} mode steers no traffic"
            return 0
            ;;
    esac
    awk -v floor="$STEERING_MIN_PACKETS_PER_SECOND" -v mode="$mode" \
        -v client_ports="$client_ports" -v replication_ports="$replication_ports" '
        # `ports <list>`: how a comma-separated list of ports is named.
        function ports(list) { return (index(list, ",") ? "ports " : "port ") list }
        BEGIN {
            if (mode == "port") {
                count = split(client_ports, list, ",")
                for (i = 1; i <= count; i++) side_of_port[list[i]] = "clients"
                count = split(replication_ports, list, ",")
                for (i = 1; i <= count; i++) side_of_port[list[i]] = "replication"
                where["clients"] = ports(client_ports)
                where["replication"] = ports(replication_ports)
                absent = "the primary has no " where["replication"] ": replication is not on a NIC of its own"
            } else {
                where["clients"] = "queue 0"
                where["replication"] = "queue 1"
                absent = "the primary has no queue 1: replication is not on a queue of its own"
            }
        }
        # `label <name>`: the value of label `name` on the current line.
        function label(name,   value) {
            value = $1
            sub(".*[{,]" name "=\"", "", value)
            sub(/".*/, "", value)
            return value
        }
        /^# sample/ { n++; next }
        n && $1 ~ /^melin_dpdk_queue_(rx|tx)_packets_total\{/ {
            lines[n]++
            dir = ($1 ~ /_rx_/) ? "in" : "out"
            if (mode == "port") {
                side = side_of_port[label("port")]
            } else {
                queue = label("queue")
                side = (queue == 1) ? "replication" : (queue == 0) ? "clients" : ""
            }
            if (side == "") next
            moved[n, dir, side] += $2
            if (side == "replication") has_replication = 1
        }
        function delta(dir, side) {
            return moved[last, dir, side] - moved[first, dir, side]
        }
        END {
            for (i = 1; i <= n; i++) if (lines[i] > fullest) fullest = lines[i]
            for (i = 1; i <= n; i++) {
                if (fullest && lines[i] == fullest) { if (!first) first = i; last = i }
            }
            if (first == last) { print "UNJUDGED: fewer than two samples of the queue counters"; exit 1 }
            if (!has_replication) { print "DEGRADED: " absent; exit 1 }
            seconds = last - first
            least = floor * seconds
            # %.0f, not %d: some awks print %d through a 32-bit int.
            summary = sprintf("%s (clients) %.0f in, %.0f out; %s (replication) %.0f in, %.0f out; over %d sampled seconds",
                where["clients"], delta("in", "clients"), delta("out", "clients"),
                where["replication"], delta("in", "replication"), delta("out", "replication"), seconds)
            if (delta("in", "replication") < least || delta("out", "replication") < least)
                reasons = reasons "; replication is not on " where["replication"]
            if (delta("in", "clients") < least || delta("out", "clients") < least)
                reasons = reasons "; client traffic is not on " where["clients"]
            if (reasons == "") { print "ok: " summary; exit 0 }
            printf "DEGRADED: %s%s (each needs %.0f packets each way, %d a second)\n",
                summary, reasons, least, floor
            exit 1
        }' "$2"
}

# ---------------------------------------------------------------------------
# TRACE=1: servers built with per-stage latency histograms, read after
# each run from the primary's /stats-dump
# ---------------------------------------------------------------------------

# `switch_setting <name> <value>`: an on/off setting's value, 1 when on
# (`1`), 0 when off (unset, empty or `0`). On anything else, says why on
# stderr, naming the setting, and fails, rather than reading a typo as
# either.
switch_setting() {
    case "${2:-}" in
        "" | 0) echo 0 ;;
        1) echo 1 ;;
        *)
            echo "error: ${1} must be 1, or 0 or unset (got '${2}')" >&2
            return 1
            ;;
    esac
}

# `trace_setting <TRACE>`: 1 when the suite runs traced servers, 0 when
# it does not (switch_setting's).
trace_setting() {
    switch_setting TRACE "${1:-}"
}

# The cargo feature a server is traced with: the runtime's per-stage
# latency histograms, served on the health endpoint's /stats-dump. Every
# application's server declares its own, forwarding to the runtime's.
TRACE_FEATURE=latency-trace

# `refuse_untraced_server <package> <cargo metadata JSON>`: whether the
# server `package` can be built traced, which needs a TRACE_FEATURE
# feature of its own, judged by `cargo metadata --no-deps` of its
# workspace. Says why on stderr and fails for one that cannot: built
# without it, the server would run untraced, and the suite would only
# find out from its dump, after every run.
refuse_untraced_server() {
    python3 -I -c '
import json, sys
package, feature = sys.argv[1], sys.argv[2]
try:
    doc = json.load(sys.stdin)
except ValueError:
    sys.exit(f"error: TRACE=1: cannot read the features of {package} (cargo metadata gave no JSON)")
found = [p for p in doc.get("packages", []) if p.get("name") == package]
if not found:
    sys.exit(f"error: TRACE=1: no package {package} in its workspace")
if feature not in found[0].get("features", {}):
    sys.exit(f"error: TRACE=1: {package} has no {feature} feature, so its server cannot be built traced (refused rather than run untraced); add one forwarding to melin-server-runtime/{feature}")
' "$1" "$TRACE_FEATURE" <<< "$2"
}

# `with_trace_feature <features> <package> <trace>`: a server build's
# cargo features (comma-separated, possibly empty), with `package`'s
# TRACE_FEATURE added when `trace` (trace_setting's) is 1, and as given
# otherwise.
with_trace_feature() {
    local features="$1"
    [[ "$3" == 1 ]] && features="${features:+${features},}${2}/${TRACE_FEATURE}"
    echo "$features"
}

# The stages the trace summary reads, as the node registers them: the
# DPDK client poll loop's, and in queue and port modes the replication
# poll thread's, each timing the loop's iterations that did work.
# test-lan-bench-suite.sh checks the node's source for both.
TRACE_CLIENT_POLL_STAGE="dpdk poll: outer iteration (work-iterations only)"
TRACE_REPL_POLL_STAGE="dpdk repl poll: outer iteration (work-iterations only)"

# `judge_trace <dump> <transport> [<DPDK replication mode>]`: the verdict
# on a run's /stats-dump, saved from the primary as it is served: one
# line per registered stage,
#   stage<TAB><name><TAB><samples><TAB><min><TAB><p50><TAB><p90><TAB><p99><TAB><p99.9><TAB><max>
# in nanoseconds, or a single `# latency-trace disabled` line from an
# untraced build, or `# no samples` before any stage registered, and a
# `# truncated<TAB><count>` line when the stages outgrew the endpoint's
# buffer. The dump must hold stages; on DPDK, the client poll loop's
# with samples, and in queue and port modes the replication poll
# thread's too (a run's load gives every loop work), or the trace pass
# did not measure what it was run for. Prints the verdict; fails unless
# it is `ok`.
judge_trace() {
    local transport="$2" mode="${3:-}" need_client=0 need_repl=0
    if [[ "$transport" == dpdk ]]; then
        need_client=1
        [[ "$mode" == queue || "$mode" == port ]] && need_repl=1
    fi
    awk -F'\t' -v client="$TRACE_CLIENT_POLL_STAGE" -v repl="$TRACE_REPL_POLL_STAGE" \
        -v need_client="$need_client" -v need_repl="$need_repl" '
        # `require <stage> <what>`: why the dump fails for lack of `stage`,
        # or nothing.
        function require(stage, what) {
            if (!(stage in samples)) return "; no " what " stage"
            if (samples[stage] == 0) return "; the " what " recorded nothing"
            return ""
        }
        /^# latency-trace disabled/ { disabled = 1 }
        $1 == "# truncated" { truncated = $2 }
        $1 == "stage" && NF == 9 { stages++; samples[$2] = $3 }
        END {
            if (NR == 0) { print "FAILED: no /stats-dump from the primary"; exit 1 }
            if (disabled) { print "FAILED: the primary was built without latency-trace"; exit 1 }
            if (!stages) { print "FAILED: no stage in the /stats-dump of the primary"; exit 1 }
            summary = stages " stages"
            if (truncated) summary = summary ", " truncated " more cut off by the endpoint"
            if (need_client) reasons = reasons require(client, "client poll loop")
            if (need_repl) reasons = reasons require(repl, "replication poll loop")
            if (reasons != "") { print "FAILED: " summary reasons; exit 1 }
            print "ok: " summary
        }' "$1"
}

# `trace_summary <dump>...`: the poll loops' work-iteration percentiles
# of each run's dump (judge_trace's format), a run being its dump's file
# name without `.stats`: the client poll loop's, then the replication
# poll thread's where the run had one, so that the modes read side by
# side. A run with neither (kernel TCP, or a dump judge_trace failed)
# says so in a line of its own.
trace_summary() {
    awk -F'\t' -v client="$TRACE_CLIENT_POLL_STAGE" -v repl="$TRACE_REPL_POLL_STAGE" '
        # `run_of <path>`: the run a dump is of.
        function run_of(path) {
            sub(/.*\//, "", path)
            sub(/\.stats$/, "", path)
            return path
        }
        # `row <run> <loop> <stage line>`: one loop of one run.
        function row(run, loop, line,   f) {
            split(line, f, "\t")
            printf fmt, run, loop, f[3], f[5], f[6], f[7], f[8], f[9]
        }
        # `fmt`: the one column format of the table, header and rows alike.
        # Every dump named, in order, an empty one (no answer) included,
        # which no input line would announce.
        BEGIN {
            fmt = "%-28s %-12s %12s %9s %9s %9s %9s %9s\n"
            for (i = 1; i < ARGC; i++) order[i] = run_of(ARGV[i])
            runs = ARGC - 1
        }
        FNR == 1 { run = run_of(FILENAME) }
        $1 == "stage" && NF == 9 {
            stages[run]++
            if ($2 == client) client_line[run] = $0
            if ($2 == repl) repl_line[run] = $0
        }
        END {
            printf fmt, "run", "loop", "iterations", "p50 ns", "p90 ns", "p99 ns", "p99.9 ns", "max ns"
            for (i = 1; i <= runs; i++) {
                run = order[i]
                if (run in client_line) row(run, "client poll", client_line[run])
                if (run in repl_line) row(run, "repl poll", repl_line[run])
                if (!(run in client_line) && !(run in repl_line))
                    printf "%-28s (no DPDK poll loop among its %d stages)\n", run, stages[run]
            }
        }' "$@"
}

# ---------------------------------------------------------------------------
# DPDK=1 in the container rig (test-containers.sh): the suite's DPDK
# passes on af_packet, over the containers' veths, configured by the rig
# itself rather than by dpdk-setup.sh
# ---------------------------------------------------------------------------

# `ipv4_as_int <a.b.c.d>`: the address as an integer. On anything else,
# an octet with a leading zero included (the node parses addresses as
# Rust's Ipv4Addr does, which refuses one), says why on stderr and fails.
# A copy of scripts/dpdk/dpdk-setup.sh's ipv4_to_int, but for the error
# message: that script is shipped to a host alone and is a root
# provisioner, not a library either side can source. Named apart so that
# test-dpdk-setup.sh, which sources both, tests the shipping one; it also
# checks that the two agree.
ipv4_as_int() {
    local o='(0|[1-9][0-9]{0,2})'
    if [[ ! "$1" =~ ^$o\.$o\.$o\.$o$ ]]; then
        echo "error: '$1' is not an IPv4 address" >&2
        return 1
    fi
    local octet value=0
    for octet in "${BASH_REMATCH[@]:1}"; do
        if (( octet > 255 )); then
            echo "error: '$1' is not an IPv4 address" >&2
            return 1
        fi
        value=$(( (value << 8) | octet ))
    done
    echo "$value"
}

# `int_to_ipv4 <integer>`: ipv4_as_int's inverse.
int_to_ipv4() {
    echo "$(( ($1 >> 24) & 255 )).$(( ($1 >> 16) & 255 )).$(( ($1 >> 8) & 255 )).$(( $1 & 255 ))"
}

# `cidr_range <a.b.c.d/len>`: the first and last addresses of the subnet,
# as integers, space-separated (host bits in the address given are
# ignored). On anything else, says why on stderr and fails.
cidr_range() {
    local ip="${1%/*}" len="${1##*/}" value size
    if [[ "$1" != */* || ! "$len" =~ ^[0-9]{1,2}$ ]] || (( 10#$len > 32 )); then
        echo "error: '$1' is not an IPv4 subnet (a.b.c.d/len)" >&2
        return 1
    fi
    value=$(ipv4_as_int "$ip") || return 1
    size=$(( 1 << (32 - 10#$len) ))
    value=$(( value - value % size ))
    echo "$value $(( value + size - 1 ))"
}

# `cidrs_overlap <a.b.c.d/len> <a.b.c.d/len>`: whether the two subnets
# share an address (exit status). Both must be well formed (cidr_range).
cidrs_overlap() {
    local a b
    a=$(cidr_range "$1") || return 1
    b=$(cidr_range "$2") || return 1
    read -ra a <<< "$a"
    read -ra b <<< "$b"
    (( a[0] <= b[1] && b[0] <= a[1] ))
}

# Where the rig's two networks are carved from: podman's own pool for
# networks it creates (its default `default_subnet_pools` base), so that
# they sit where any podman network would, a /24 each.
REHEARSAL_POOL=10.89
# The prefix length every DPDK NIC of the rehearsal is given: the pool's,
# a /16 holding both of the rig's /24s. The node takes its replication
# NIC's address to be in the client NIC's subnet (it gives the replication
# NIC the client NIC's prefix length), and the two NICs are on the two
# networks, so the client NIC's subnet must span both. Wider than either
# network, which costs nothing: a DPDK NIC resolves only the peers it
# talks to, each on its own network.
REHEARSAL_DPDK_PREFIX_LEN=16

# `rehearsal_subnets <subnet>...`: the rig's two networks (the clients'
# and replication's), two /24s of REHEARSAL_POOL.0.0/16 overlapping none
# of the subnets given (every existing podman network's; IPv6 ones are
# skipped), as `<clients' subnet> <replication's subnet>`. Taken from the
# top of the pool down, as podman allocates from the bottom up. On a pool
# with no two free, or a subnet that is not one, says why on stderr and
# fails.
rehearsal_subnets() {
    local existing=() subnet third candidate taken
    for subnet in "$@"; do
        [[ "$subnet" == *:* ]] && continue
        cidr_range "$subnet" >/dev/null || return 1
        existing+=("$subnet")
    done
    for (( third = 254; third >= 0; third -= 2 )); do
        taken=
        for candidate in "${REHEARSAL_POOL}.${third}.0/24" "${REHEARSAL_POOL}.$((third + 1)).0/24"; do
            for subnet in "${existing[@]}"; do
                cidrs_overlap "$candidate" "$subnet" && taken=1
            done
        done
        if [[ -z "$taken" ]]; then
            echo "${REHEARSAL_POOL}.${third}.0/24 ${REHEARSAL_POOL}.$((third + 1)).0/24"
            return 0
        fi
    done
    echo "error: no two free /24s in ${REHEARSAL_POOL}.0.0/16 for the rehearsal's networks (podman's networks hold ${existing[*]}); remove some with podman network rm" >&2
    return 1
}

# `podman_subnets <JSON>`: the subnets of `podman network inspect`'s
# networks (its JSON, one array of every network), space-separated, IPv6
# ones included. Says why on stderr and fails on JSON it cannot read.
podman_subnets() {
    python3 -I -c '
import json, sys
try:
    networks = json.load(sys.stdin)
    print(" ".join(s["subnet"] for n in networks for s in (n.get("subnets") or [])))
except (ValueError, TypeError, KeyError, AttributeError):
    sys.exit("error: cannot read the subnets of the podman networks (podman network inspect gave no list of networks with subnets)")
' <<< "$1"
}

# `lower_half <a.b.c.d/len>`: the subnet's lower half, as a subnet: the
# addresses podman is told to allocate from (`--ip-range`), leaving the
# upper half to high_free_addresses.
lower_half() {
    local range
    range=$(cidr_range "$1") || return 1
    echo "$(int_to_ipv4 "${range%% *}")/$(( 10#${1##*/} + 1 ))"
}

# `high_free_addresses <subnet> <count> [<taken address>...]`: the
# `count` highest host addresses of the subnet (its broadcast address
# excluded) that are not taken, highest first, space-separated: the
# addresses DPDK takes beside the kernel's, which podman assigned from
# the subnet's lower half. Fails, saying why on stderr, when the subnet
# has too few.
high_free_addresses() {
    local subnet="$1" count="$2" range address candidate found=()
    shift 2
    range=$(cidr_range "$subnet") || return 1
    read -ra range <<< "$range"
    local taken=" $* "
    address=$(( range[1] - 1 ))
    while (( address > range[0] && ${#found[@]} < count )); do
        candidate=$(int_to_ipv4 "$address")
        [[ "$taken" != *" ${candidate} "* ]] && found+=("$candidate")
        address=$(( address - 1 ))
    done
    if (( ${#found[@]} < count )); then
        echo "error: ${subnet} has fewer than ${count} free addresses" >&2
        return 1
    fi
    echo "${found[*]}"
}

# The af_packet devices of the rehearsal's DPDK NICs: the client NIC's on
# every container, the replication NIC's beside it on a node's. EAL
# numbers virtual devices' ports in the order DPDK_EAL_ARGS creates them,
# the client NIC's first: port 0, and the replication NIC's port 1.
REHEARSAL_CLIENT_VDEV=net_af_packet0
REHEARSAL_REPL_VDEV=net_af_packet1
# The memory, in MiB, each DPDK process takes without hugepages: what the
# netns test runner gives a process holding several nodes' mbuf pools
# (crates/core/test-node's layout), so ample for one node's two. Reserved
# rather than touched: a process uses what its pools fill.
REHEARSAL_DPDK_MEMORY_MB=512

# `af_packet_conf <cpus> <iface> <ip> <mac> [<repl iface> <repl ip>
# <repl mac>]`: the /etc/melin-dpdk.conf the rig writes on a container,
# its DPDK on af_packet: the client NIC on `iface` (the kernel's
# interface, whose MAC DPDK shares, and beside whose address it takes
# `ip`), and on a node the replication NIC on `repl iface`, likewise.
# `cpus` is the container's CPU list (Cpus_allowed_list's form, `0-3,6`):
# EAL pins its main thread to its main lcore's CPUs, and every thread the
# process starts after inherits them, so the one lcore is given all of
# them rather than one core shared by every thread of every container.
# The EAL arguments are the netns test runner's (no hugepages, no PCI
# scan), without `--in-memory`, which EAL refuses beside `--no-huge`: the
# runtime directory it would keep in memory is the container's own, and
# the suite clears it between runs. On arguments it cannot use, says why
# on stderr and fails.
af_packet_conf() {
    local cpus="$1" iface="$2" ip="$3" mac="$4" repl_iface="${5:-}" repl_ip="${6:-}" repl_mac="${7:-}"
    if [[ ! "$cpus" =~ ^[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*$ ]]; then
        echo "error: '${cpus}' is not a CPU list (as Cpus_allowed_list gives one: 0-3,6)" >&2
        return 1
    fi
    if [[ -z "$iface" || -z "$ip" || -z "$mac" ]]; then
        echo "error: the client NIC needs an interface, an address and a MAC" >&2
        return 1
    fi
    if [[ -n "${repl_iface}${repl_ip}${repl_mac}" && ( -z "$repl_iface" || -z "$repl_ip" || -z "$repl_mac" ) ]]; then
        echo "error: the replication NIC needs an interface, an address and a MAC, or none of them" >&2
        return 1
    fi
    local eal="--no-huge -m ${REHEARSAL_DPDK_MEMORY_MB} --no-pci --lcores=0@(${cpus}) --vdev=${REHEARSAL_CLIENT_VDEV},iface=${iface}"
    [[ -n "$repl_iface" ]] && eal+=" --vdev=${REHEARSAL_REPL_VDEV},iface=${repl_iface}"
    cat <<EOF
DPDK_IP=${ip}
DPDK_PREFIX=${REHEARSAL_DPDK_PREFIX_LEN}
DPDK_PORT=0
DPDK_MODE=af_packet
DPDK_NIC=${iface}
DPDK_MAC=${mac}
DPDK_EAL_ARGS="${eal}"
EOF
    [[ -z "$repl_iface" ]] && return 0
    cat <<EOF
DPDK_REPL_NIC=${repl_iface}
DPDK_REPL_VDEV=${REHEARSAL_REPL_VDEV}
DPDK_REPL_IP=${repl_ip}
DPDK_REPL_MAC=${repl_mac}
DPDK_REPL_PORT=1
EOF
}

# CAP_NET_RAW's bit in a capability set: af_packet's sockets need it.
CAP_NET_RAW=13
# CAP_NET_ADMIN's: af_packet's promiscuous switch needs it, and starting
# a port flips it (test-containers.sh says why that is fatal).
CAP_NET_ADMIN=12

# `has_capability <capability set> <bit>`: whether the set (hexadecimal,
# as /proc/<pid>/status's CapEff gives it) holds the capability (exit
# status).
has_capability() {
    [[ "$1" =~ ^[0-9a-fA-F]{1,16}$ ]] && (( (16#$1 >> $2) & 1 ))
}
