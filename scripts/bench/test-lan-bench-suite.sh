#!/usr/bin/env bash
# Tests for the LAN suite's host-free logic (lib.sh): the health and
# steering verdicts that decide whether a run passed, the reading of a
# host's DPDK configuration and whether it can run queue or port mode,
# the DPDK_REPLICATION setting and the node flags each mode adds, the
# node log line the suite waits for, and TRACE=1: the setting, the
# servers' trace feature, and the verdict on and summary of a traced
# primary's /stats-dump, with the stage names and format it reads checked
# against the node's source.
# The rest of the suite drives hosts and is exercised by running it.
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

# `samples <samples...>`: a samples file as the suite's sampler writes
# it, from samples given one per argument, `;` between metric lines.
# Prints its path.
samples() {
    local file="${TMP_ROOT}/samples" sample
    : > "$file"
    for sample in "$@"; do
        echo "# sample" >> "$file"
        tr ';' '\n' <<< "$sample" | sed '/^$/d' >> "$file"
    done
    echo "$file"
}

# `status <command...>`: the command's exit status, then its stdout and
# stderr as one line.
status() {
    local out rc=0
    out=$("$@" 2>&1) || rc=$?
    echo "${rc} ${out}"
}

# `verdict <expected replicas> <samples...>`: judge_health's line and
# status on `samples`.
verdict() {
    local expected="$1"
    shift
    status judge_health "$(samples "$@")" "$expected"
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
echo "=== dpdk_conf_mode ==="
check "an mlx5 host" "mlx5" "$(dpdk_conf_mode "DPDK_IP=10.0.0.1
DPDK_MODE=mlx5")"
check "an L3 bifurcated host" "l3" "$(dpdk_conf_mode "DPDK_MODE=l3")"
check "an SR-IOV host" "sriov" "$(dpdk_conf_mode "DPDK_MODE=sriov")"
check "a conf naming no mode is SR-IOV" "sriov" "$(dpdk_conf_mode "$CONF")"
check "no conf at all is SR-IOV, the only setup the suite runs unattended" "sriov" "$(dpdk_conf_mode "")"

echo ""
echo "=== dpdk_replication_modes ==="
check "unset is shared, as before the setting" "0 shared" "$(status dpdk_replication_modes "")"
check "one mode" "0 queue" "$(status dpdk_replication_modes queue)"
check "all is every mode the suite runs" "0 shared queue port" "$(status dpdk_replication_modes all)"
check "the order given is the order run" "0 queue shared" "$(status dpdk_replication_modes queue,shared)"
check "port mode is one of them" "0 port shared" "$(status dpdk_replication_modes port,shared)"
check "a trailing comma is ignored" "0 shared" "$(status dpdk_replication_modes shared,)"
check "an unknown mode is refused" \
    "1 error: unknown DPDK_REPLICATION mode 'qeue': shared queue port" \
    "$(status dpdk_replication_modes shared,qeue)"
check "an empty mode inside the list is refused" \
    "1 error: unknown DPDK_REPLICATION mode '': shared queue port" \
    "$(status dpdk_replication_modes shared,,queue)"
check "a mode listed twice is refused" \
    "1 error: DPDK_REPLICATION lists 'shared' twice" \
    "$(status dpdk_replication_modes shared,queue,shared)"
check "port mode listed twice is refused" \
    "1 error: DPDK_REPLICATION lists 'port' twice" \
    "$(status dpdk_replication_modes port,queue,port)"
check "modes separated by a space are one unknown mode" \
    "1 error: unknown DPDK_REPLICATION mode 'shared queue': shared queue port" \
    "$(status dpdk_replication_modes "shared queue")"
check "a space cannot slip a second copy past the duplicate check" \
    "1 error: unknown DPDK_REPLICATION mode 'shared queue': shared queue port" \
    "$(status dpdk_replication_modes "shared,shared queue")"

echo ""
echo "=== refuse_queue_mode ==="
check "an mlx5 host runs queue mode" "0 " "$(status refuse_queue_mode root@h1 "DPDK_MODE=mlx5")"
check "an L3 bifurcated host runs queue mode" "0 " "$(status refuse_queue_mode root@h1 "DPDK_MODE=l3")"
check "an SR-IOV host is refused, by name" \
    "1 error: DPDK_REPLICATION=queue: root@h1 runs DPDK on SR-IOV (DPDK_MODE=sriov in /etc/melin-dpdk.conf), whose VFs cannot steer replication to a queue of its own; queue mode needs mlx5" \
    "$(status refuse_queue_mode root@h1 "DPDK_MODE=sriov")"
check "a host without a conf is refused, by name" \
    "1 error: DPDK_REPLICATION=queue: root@h2 has no /etc/melin-dpdk.conf, and the suite's unattended setup cannot configure queue mode (it sets up SR-IOV on a bond, whose VFs cannot steer replication to a queue of its own, and mlx5 needs DPDK_IP); run scripts/dpdk/dpdk-setup.sh with DPDK_IP there first" \
    "$(status refuse_queue_mode root@h2 "")"

echo ""
echo "=== refuse_port_mode ==="
# What dpdk-setup.sh writes on an mlx5 host given a replication NIC
# (test-dpdk-setup.sh checks that it does): the client NIC on DPDK port
# 1, the replication NIC on port 0, lower on the bus.
PORT_CONF='DPDK_IP=10.0.0.10
DPDK_PREFIX=24
DPDK_PORT=1
DPDK_MODE=mlx5
DPDK_PCI=0000:c1:00.1
DPDK_MAC=0c:42:a1:00:00:01
DPDK_EAL_ARGS="-a 0000:c1:00.1 -a 0000:c1:00.0 --huge-dir=/mnt/huge_2m"
DPDK_REPL_PCI=0000:c1:00.0
DPDK_REPL_IP=10.0.0.20
DPDK_REPL_MAC=0c:42:a1:00:00:00
DPDK_REPL_PORT=0'
PORT_SETUP="run scripts/dpdk/dpdk-setup.sh with DPDK_IP, DPDK_REPL_NIC and DPDK_REPL_IP there (a second mlx5 port, see docs/benchmarking.md)"
check "a host with a replication NIC runs port mode" "0 " "$(status refuse_port_mode root@h1 "$PORT_CONF")"
check "a host with one NIC is refused, by name" \
    "1 error: DPDK_REPLICATION=port: root@h1's /etc/melin-dpdk.conf has no DPDK_REPL_PORT DPDK_REPL_IP DPDK_REPL_MAC DPDK_REPL_PCI, so no NIC of replication's own; ${PORT_SETUP}" \
    "$(status refuse_port_mode root@h1 "DPDK_MODE=mlx5
DPDK_IP=10.0.0.10")"
check "a host missing one key is refused, naming it" \
    "1 error: DPDK_REPLICATION=port: root@h1's /etc/melin-dpdk.conf has no DPDK_REPL_MAC, so no NIC of replication's own; ${PORT_SETUP}" \
    "$(status refuse_port_mode root@h1 "$(grep -v '^DPDK_REPL_MAC=' <<< "$PORT_CONF")")"
check "a host without a conf is refused, by name" \
    "1 error: DPDK_REPLICATION=port: root@h2 has no /etc/melin-dpdk.conf, so no NIC of replication's own; ${PORT_SETUP}" \
    "$(status refuse_port_mode root@h2 "")"
check "a host whose EAL does not allow the replication NIC is refused" \
    "1 error: DPDK_REPLICATION=port: root@h1's /etc/melin-dpdk.conf has DPDK_EAL_ARGS not allowing the replication NIC's device (-a 0000:c1:00.0); ${PORT_SETUP}" \
    "$(status refuse_port_mode root@h1 "$(sed 's/ -a 0000:c1:00.0//' <<< "$PORT_CONF")")"
check "a device allowed with devargs is allowed" "0 " \
    "$(status refuse_port_mode root@h1 "$(sed 's/-a 0000:c1:00.0 /-a 0000:c1:00.0,dv_flow_en=1 /' <<< "$PORT_CONF")")"
check "a device allowed is matched whole, not by prefix" \
    "1 error: DPDK_REPLICATION=port: root@h1's /etc/melin-dpdk.conf has DPDK_EAL_ARGS not allowing the replication NIC's device (-a 0000:c1:00.0); ${PORT_SETUP}" \
    "$(status refuse_port_mode root@h1 "$(sed 's/-a 0000:c1:00.0 /-a 0000:c1:00.00 /' <<< "$PORT_CONF")")"

echo ""
echo "=== replication_flags ==="
check "shared mode adds nothing" "" "$(replication_flags shared 0 10.0.0.20)"
check "queue mode adds the queue flag" " --dpdk-replication-queue" "$(replication_flags queue "" "")"
check "a port-mode primary names its replication NIC" \
    " --dpdk-replication-ports 0 --dpdk-replication-ip 10.0.0.20" \
    "$(replication_flags port 0 10.0.0.20)"
check "a port-mode replica names its own, and the primary's MAC its own dials" \
    " --dpdk-replication-ports 1 --dpdk-replication-ip 10.0.0.21 --dpdk-replication-peer-mac 0c:42:a1:00:00:00" \
    "$(replication_flags port 1 10.0.0.21 0c:42:a1:00:00:00)"

echo ""
echo "=== judge_steering ==="
# `queues <port> <q0 in> <q0 out> <q1 in> <q1 out>`: one port's counter
# lines, as one sample's `;`-separated lines. An empty count leaves the
# line out.
queues() {
    local port="$1" dir queue count lines=""
    shift
    for queue in 0 1; do
        for dir in rx tx; do
            count="$1"
            shift
            [[ -z "$count" ]] && continue
            lines+="melin_dpdk_queue_${dir}_packets_total{port=\"${port}\",queue=\"${queue}\"} ${count};"
        done
    done
    echo "$lines"
}

# `steering <mode> <samples...>`: judge_steering's line and status.
steering() {
    local mode="$1"
    shift
    status judge_steering "$mode" "$(samples "$@")"
}

check "replication on queue 1 and clients on queue 0 pass" \
    "0 ok: queue 0 (clients) 10000 in, 9000 out; queue 1 (replication) 18000 in, 20000 out; over 2 sampled seconds" \
    "$(steering queue "$(queues 0 0 0 0 0)" "$(queues 0 5000 4500 9000 10000)" "$(queues 0 10000 9000 18000 20000)")"
check "the verdict is on what moved during the run, not since the node started" \
    "0 ok: queue 0 (clients) 10000 in, 9000 out; queue 1 (replication) 18000 in, 20000 out; over 2 sampled seconds" \
    "$(steering queue "$(queues 0 70 60 900000 800000)" "$(queues 0 5070 4560 909000 810000)" "$(queues 0 10070 9060 918000 820000)")"
check "replication left on queue 0, queue 1 seeing heartbeats, degrades the run" \
    "1 DEGRADED: queue 0 (clients) 30000 in, 30000 out; queue 1 (replication) 2 in, 2 out; over 2 sampled seconds; replication is not on queue 1 (each needs 200 packets each way, 100 a second)" \
    "$(steering queue "$(queues 0 0 0 5 5)" "$(queues 0 15000 15000 6 6)" "$(queues 0 30000 30000 7 7)")"
check "replication sent on queue 1 with its acks arriving elsewhere degrades the run" \
    "1 DEGRADED: queue 0 (clients) 30000 in, 10000 out; queue 1 (replication) 3 in, 20000 out; over 2 sampled seconds; replication is not on queue 1 (each needs 200 packets each way, 100 a second)" \
    "$(steering queue "$(queues 0 0 0 0 0)" "$(queues 0 15000 5000 1 10000)" "$(queues 0 30000 10000 3 20000)")"
check "replication sent on queue 0 with its acks steered to queue 1 degrades the run" \
    "1 DEGRADED: queue 0 (clients) 30000 in, 30000 out; queue 1 (replication) 20000 in, 3 out; over 2 sampled seconds; replication is not on queue 1 (each needs 200 packets each way, 100 a second)" \
    "$(steering queue "$(queues 0 0 0 0 0)" "$(queues 0 15000 15000 10000 1)" "$(queues 0 30000 30000 20000 3)")"
check "client requests arriving on queue 0 with no replies sent from it degrade the run" \
    "1 DEGRADED: queue 0 (clients) 20000 in, 2 out; queue 1 (replication) 40000 in, 40000 out; over 2 sampled seconds; client traffic is not on queue 0 (each needs 200 packets each way, 100 a second)" \
    "$(steering queue "$(queues 0 0 0 0 0)" "$(queues 0 10000 1 20000 20000)" "$(queues 0 20000 2 40000 40000)")"
check "client traffic not on queue 0, queue 0 seeing ARP, degrades the run" \
    "1 DEGRADED: queue 0 (clients) 4 in, 0 out; queue 1 (replication) 40000 in, 40000 out; over 2 sampled seconds; client traffic is not on queue 0 (each needs 200 packets each way, 100 a second)" \
    "$(steering queue "$(queues 0 10 0 0 0)" "$(queues 0 12 0 20000 20000)" "$(queues 0 14 0 40000 40000)")"
check "a run with nothing moving anywhere names both" \
    "1 DEGRADED: queue 0 (clients) 3 in, 3 out; queue 1 (replication) 1 in, 1 out; over 2 sampled seconds; replication is not on queue 1; client traffic is not on queue 0 (each needs 200 packets each way, 100 a second)" \
    "$(steering queue "$(queues 0 0 0 0 0)" "$(queues 0 1 1 0 0)" "$(queues 0 3 3 1 1)")"
check "a primary with no queue 1 is not in queue mode" \
    "1 DEGRADED: the primary has no queue 1: replication is not on a queue of its own" \
    "$(steering queue "$(queues 0 0 0 "" "")" "$(queues 0 50000 50000 "" "")")"
check "exactly the least a queue must move passes" \
    "0 ok: queue 0 (clients) 200 in, 200 out; queue 1 (replication) 200 in, 200 out; over 2 sampled seconds" \
    "$(steering queue "$(queues 0 0 0 0 0)" "" "$(queues 0 200 200 200 200)")"
check "a bond's ports are summed, its transmit on the first port only" \
    "0 ok: queue 0 (clients) 300 in, 300 out; queue 1 (replication) 300 in, 300 out; over 2 sampled seconds" \
    "$(steering queue "$(queues 0 0 0 0 0)$(queues 1 0 0 0 0)" "" \
        "$(queues 0 150 300 150 300)$(queues 1 150 0 150 0)")"
check "a bond's port short of the rest on its own still passes on the sum" \
    "0 ok: queue 0 (clients) 250 in, 300 out; queue 1 (replication) 250 in, 300 out; over 2 sampled seconds" \
    "$(steering queue "$(queues 0 0 0 0 0)$(queues 1 0 0 0 0)" "" \
        "$(queues 0 240 300 240 300)$(queues 1 10 0 10 0)")"
check "a scrape cut short is not used" \
    "0 ok: queue 0 (clients) 10000 in, 10000 out; queue 1 (replication) 20000 in, 20000 out; over 1 sampled seconds" \
    "$(steering queue "$(queues 0 0 0 0 0)" "$(queues 0 10000 10000 20000 20000)" "$(queues 0 90000 "" "" "")")"
check "counts past 32 bits are exact" \
    "0 ok: queue 0 (clients) 5000000000 in, 5000000000 out; queue 1 (replication) 6000000000 in, 6000000000 out; over 1 sampled seconds" \
    "$(steering queue "$(queues 0 0 0 0 0)" "$(queues 0 5000000000 5000000000 6000000000 6000000000)")"
check "one complete sample is unjudged" \
    "1 UNJUDGED: fewer than two samples of the queue counters" \
    "$(steering queue "" "$(queues 0 10 10 10 10)" "")"
check "no sample is unjudged" \
    "1 UNJUDGED: fewer than two samples of the queue counters" \
    "$(steering queue)"
check "shared mode steers nothing and is not judged" \
    "0 not judged: shared mode steers no traffic" \
    "$(steering shared "$(queues 0 0 0 "" "")" "$(queues 0 50000 50000 "" "")")"

# Port mode: replication on a NIC of its own. As a node in port mode
# labels them, one queue (0) per port: the client NIC's port and the
# replication NIC's.
# `nics <client port> <in> <out> <replication port> <in> <out>`: one
# sample's lines of both NICs. An empty replication port leaves its NIC
# out, as on a primary with none.
nics() {
    local lines
    lines="$(queues "$1" "$2" "$3" "" "")"
    [[ -n "$4" ]] && lines+="$(queues "$4" "$5" "$6" "" "")"
    echo "$lines"
}

# `port_steering <client ports> <replication ports> <samples...>`:
# judge_steering's line and status in port mode.
port_steering() {
    local client="$1" replication="$2"
    shift 2
    status judge_steering port "$(samples "$@")" "$client" "$replication"
}

check "port mode: replication on its NIC and clients on theirs pass" \
    "0 ok: port 0 (clients) 10000 in, 9000 out; port 1 (replication) 18000 in, 20000 out; over 2 sampled seconds" \
    "$(port_steering 0 1 "$(nics 0 0 0 1 0 0)" "$(nics 0 5000 4500 1 9000 10000)" "$(nics 0 10000 9000 1 18000 20000)")"
check "port mode: the client NIC need not be port 0" \
    "0 ok: port 1 (clients) 10000 in, 9000 out; port 0 (replication) 18000 in, 20000 out; over 1 sampled seconds" \
    "$(port_steering 1 0 "$(nics 1 0 0 0 0 0)" "$(nics 1 10000 9000 0 18000 20000)")"
check "port mode: replication left on the client NIC, its own seeing heartbeats, degrades the run" \
    "1 DEGRADED: port 0 (clients) 30000 in, 30000 out; port 1 (replication) 2 in, 2 out; over 2 sampled seconds; replication is not on port 1 (each needs 200 packets each way, 100 a second)" \
    "$(port_steering 0 1 "$(nics 0 0 0 1 5 5)" "$(nics 0 15000 15000 1 6 6)" "$(nics 0 30000 30000 1 7 7)")"
check "port mode: replication sent on its NIC with the acks arriving on the client NIC degrades the run" \
    "1 DEGRADED: port 0 (clients) 30000 in, 10000 out; port 1 (replication) 3 in, 20000 out; over 2 sampled seconds; replication is not on port 1 (each needs 200 packets each way, 100 a second)" \
    "$(port_steering 0 1 "$(nics 0 0 0 1 0 0)" "$(nics 0 15000 5000 1 1 10000)" "$(nics 0 30000 10000 1 3 20000)")"
check "port mode: replication sent on the client NIC with the acks on its own degrades the run" \
    "1 DEGRADED: port 0 (clients) 30000 in, 30000 out; port 1 (replication) 20000 in, 3 out; over 2 sampled seconds; replication is not on port 1 (each needs 200 packets each way, 100 a second)" \
    "$(port_steering 0 1 "$(nics 0 0 0 1 0 0)" "$(nics 0 15000 15000 1 10000 1)" "$(nics 0 30000 30000 1 20000 3)")"
check "port mode: client requests arriving with no replies sent degrade the run" \
    "1 DEGRADED: port 0 (clients) 20000 in, 2 out; port 1 (replication) 40000 in, 40000 out; over 2 sampled seconds; client traffic is not on port 0 (each needs 200 packets each way, 100 a second)" \
    "$(port_steering 0 1 "$(nics 0 0 0 1 0 0)" "$(nics 0 10000 1 1 20000 20000)" "$(nics 0 20000 2 1 40000 40000)")"
check "port mode: client traffic not on the client NIC, which sees ARP, degrades the run" \
    "1 DEGRADED: port 0 (clients) 4 in, 0 out; port 1 (replication) 40000 in, 40000 out; over 2 sampled seconds; client traffic is not on port 0 (each needs 200 packets each way, 100 a second)" \
    "$(port_steering 0 1 "$(nics 0 10 0 1 0 0)" "$(nics 0 12 0 1 20000 20000)" "$(nics 0 14 0 1 40000 40000)")"
check "port mode: a run with nothing moving anywhere names both" \
    "1 DEGRADED: port 0 (clients) 3 in, 3 out; port 1 (replication) 1 in, 1 out; over 2 sampled seconds; replication is not on port 1; client traffic is not on port 0 (each needs 200 packets each way, 100 a second)" \
    "$(port_steering 0 1 "$(nics 0 0 0 1 0 0)" "$(nics 0 1 1 1 0 0)" "$(nics 0 3 3 1 1 1)")"
check "port mode: a primary with no replication port is not in port mode" \
    "1 DEGRADED: the primary has no port 1: replication is not on a NIC of its own" \
    "$(port_steering 0 1 "$(nics 0 0 0 "")" "$(nics 0 50000 50000 "")")"
check "port mode: replication on a second queue of the client NIC is not on a NIC of its own" \
    "1 DEGRADED: the primary has no port 1: replication is not on a NIC of its own" \
    "$(port_steering 0 1 "$(queues 0 0 0 0 0)" "$(queues 0 10000 10000 20000 20000)")"
check "port mode: exactly the least each NIC must move passes" \
    "0 ok: port 0 (clients) 200 in, 200 out; port 1 (replication) 200 in, 200 out; over 2 sampled seconds" \
    "$(port_steering 0 1 "$(nics 0 0 0 1 0 0)" "" "$(nics 0 200 200 1 200 200)")"
check "port mode: a NIC's ports are summed and named" \
    "0 ok: ports 0,1 (clients) 300 in, 300 out; ports 2,3 (replication) 300 in, 300 out; over 1 sampled seconds" \
    "$(port_steering 0,1 2,3 "$(nics 0 0 0 2 0 0)$(nics 1 0 0 3 0 0)" \
        "$(nics 0 150 300 2 150 300)$(nics 1 150 0 3 150 0)")"
check "port mode: a port of neither NIC is not counted" \
    "0 ok: port 0 (clients) 1000 in, 1000 out; port 1 (replication) 1000 in, 1000 out; over 1 sampled seconds" \
    "$(port_steering 0 1 "$(nics 0 0 0 1 0 0)$(queues 7 0 0 "" "")" \
        "$(nics 0 1000 1000 1 1000 1000)$(queues 7 99999 99999 "" "")")"
check "port mode: a scrape cut short is not used" \
    "0 ok: port 0 (clients) 10000 in, 10000 out; port 1 (replication) 20000 in, 20000 out; over 1 sampled seconds" \
    "$(port_steering 0 1 "$(nics 0 0 0 1 0 0)" "$(nics 0 10000 10000 1 20000 20000)" "$(nics 0 90000 "" "")")"
check "port mode: one complete sample is unjudged" \
    "1 UNJUDGED: fewer than two samples of the queue counters" \
    "$(port_steering 0 1 "$(nics 0 10 10 1 10 10)")"
check "port mode without the client NIC's ports is unjudged" \
    "1 UNJUDGED: port mode needs the client and replication NICs' DPDK ports" \
    "$(port_steering "" 1 "$(nics 0 0 0 1 0 0)" "$(nics 0 1000 1000 1 1000 1000)")"
check "port mode without the replication NIC's ports is unjudged" \
    "1 UNJUDGED: port mode needs the client and replication NICs' DPDK ports" \
    "$(port_steering 0 "" "$(nics 0 0 0 1 0 0)" "$(nics 0 1000 1000 1 1000 1000)")"
check "queue mode ignores the ports given, judging queues" \
    "0 ok: queue 0 (clients) 1000 in, 1000 out; queue 1 (replication) 1000 in, 1000 out; over 1 sampled seconds" \
    "$(status judge_steering queue "$(samples "$(queues 0 0 0 0 0)" "$(queues 0 1000 1000 1000 1000)")" 0 1)"

echo ""
echo "=== the primary's readiness line ==="
# The suite waits for this text on a DPDK primary in every replication
# mode: each of the node's lines announcing the replication sender must
# carry it, the shared mode's and the split modes'.
SERVER_RS="${SCRIPT_DIR}/../../crates/core/server-runtime/src/server.rs"
check "both of the DPDK primary's replication lines carry the text waited for" "2" \
    "$(grep -c "info!(addr = %repl_bind, \"${DPDK_REPLICATION_READY}" "$SERVER_RS")"

echo ""
echo "=== trace_setting ==="
check "unset is untraced, as before the setting" "0 0" "$(status trace_setting "")"
check "0 is untraced" "0 0" "$(status trace_setting 0)"
check "1 is traced" "0 1" "$(status trace_setting 1)"
check "anything else is refused, not read as either" \
    "1 error: TRACE must be 1, or 0 or unset (got 'yes')" "$(status trace_setting yes)"

echo ""
echo "=== with_trace_feature ==="
check "untraced, the features are as given" "synthetic-seed" \
    "$(with_trace_feature synthetic-seed melin-ec-server 0)"
check "untraced, no features stay none" "" "$(with_trace_feature "" melin-example-echo 0)"
check "traced, the server's own feature is added" \
    "melin-example-echo/dpdk,melin-example-echo/latency-trace" \
    "$(with_trace_feature melin-example-echo/dpdk melin-example-echo 1)"
check "traced, with no features of its own, the trace feature alone" \
    "melin-example-notary/latency-trace" "$(with_trace_feature "" melin-example-notary 1)"

echo ""
echo "=== refuse_untraced_server ==="
# `metadata <package> <features JSON>`: the part of `cargo metadata
# --no-deps --format-version 1` the refusal reads, for one package.
metadata() {
    printf '{"packages":[{"name":"other","features":{"latency-trace":[]}},{"name":"%s","features":%s}],"workspace_members":[],"version":1}' "$1" "$2"
}
check "a server with the feature is built traced" "0 " \
    "$(status refuse_untraced_server melin-ec-server "$(metadata melin-ec-server '{"default":["hash-chain"],"latency-trace":["melin-server-runtime/latency-trace"]}')")"
check "a server without it is refused, by package" \
    "1 error: TRACE=1: melin-example-echo has no latency-trace feature, so its server cannot be built traced (refused rather than run untraced); add one forwarding to melin-server-runtime/latency-trace" \
    "$(status refuse_untraced_server melin-example-echo "$(metadata melin-example-echo '{"dpdk":[]}')")"
check "another package's feature does not count" \
    "1 error: TRACE=1: melin-example-echo has no latency-trace feature, so its server cannot be built traced (refused rather than run untraced); add one forwarding to melin-server-runtime/latency-trace" \
    "$(status refuse_untraced_server melin-example-echo "$(metadata melin-example-echo '{}')")"
check "a package not in the workspace is refused" \
    "1 error: TRACE=1: no package melin-ec-server in its workspace" \
    "$(status refuse_untraced_server melin-ec-server "$(metadata melin-example-echo '{}')")"
check "no metadata is refused" \
    "1 error: TRACE=1: cannot read the features of melin-ec-server (cargo metadata gave no JSON)" \
    "$(status refuse_untraced_server melin-ec-server "")"
# This repository's servers, as the suite asks cargo about them: each has
# the feature, forwarding to the runtime's (a feature that forwarded
# nowhere would build a server whose dump is empty of stages).
REPO_METADATA=$(cargo metadata --no-deps --format-version 1 --manifest-path "${SCRIPT_DIR}/../../Cargo.toml")
for package in melin-example-echo melin-example-notary; do
    check "${package} is built traced" "0 " "$(status refuse_untraced_server "$package" "$REPO_METADATA")"
    check "${package}'s trace feature forwards to the runtime's" "melin-server-runtime/latency-trace" \
        "$(python3 -I -c 'import json, sys; print(*[" ".join(p["features"]["latency-trace"]) for p in json.load(sys.stdin)["packages"] if p["name"] == sys.argv[1]])' "$package" <<< "$REPO_METADATA")"
done

# `stage <name> <samples> <min> <p50> <p90> <p99> <p99.9> <max>`: one
# line of a /stats-dump body, as the health endpoint writes it, without
# its newline.
stage() {
    printf 'stage\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' "$@"
}
# `dump <file> <lines...>`: a dump file, under TMP_ROOT, from its lines,
# each newline-terminated as the endpoint's are; none is an empty body.
# Prints its path.
dump() {
    local file="${TMP_ROOT}/$1"
    shift
    : > "$file"
    (( $# > 0 )) && printf '%s\n' "$@" > "$file"
    echo "$file"
}
# What a traced DPDK primary's dump holds besides its poll loops, in the
# node's own stage names.
PIPELINE_STAGES=(
    "$(stage "journal: disruptor wakeup (publish → journal consume)" 120000 80 410 900 2100 5200 41000)"
    "$(stage "journal: batch processing (write + sync)" 9000 3000 11000 19000 40000 81000 300000)"
    "$(stage "matching: execute (dispatch)" 120000 20 60 90 200 900 12000)"
    "$(stage "dpdk: publish (decode → disruptor publish)" 120000 30 90 150 400 1100 9000)"
)
CLIENT_POLL="$(stage "$TRACE_CLIENT_POLL_STAGE" 98000 210 640 1500 4100 9800 52000)"
REPL_POLL="$(stage "$TRACE_REPL_POLL_STAGE" 41000 180 520 1100 2600 6100 30000)"

echo ""
echo "=== judge_trace ==="
check "a shared-mode dump with the client poll loop passes" "0 ok: 5 stages" \
    "$(status judge_trace "$(dump d "${PIPELINE_STAGES[@]}" "$CLIENT_POLL")" dpdk shared)"
check "a queue-mode dump with both poll loops passes" "0 ok: 6 stages" \
    "$(status judge_trace "$(dump d "${PIPELINE_STAGES[@]}" "$CLIENT_POLL" "$REPL_POLL")" dpdk queue)"
check "a port-mode dump with both poll loops passes" "0 ok: 6 stages" \
    "$(status judge_trace "$(dump d "${PIPELINE_STAGES[@]}" "$REPL_POLL" "$CLIENT_POLL")" dpdk port)"
check "a queue-mode dump without the replication poll loop fails" \
    "1 FAILED: 5 stages; no replication poll loop stage" \
    "$(status judge_trace "$(dump d "${PIPELINE_STAGES[@]}" "$CLIENT_POLL")" dpdk queue)"
check "a port-mode dump whose replication poll loop recorded nothing fails" \
    "1 FAILED: 6 stages; the replication poll loop recorded nothing" \
    "$(status judge_trace "$(dump d "${PIPELINE_STAGES[@]}" "$CLIENT_POLL" "$(stage "$TRACE_REPL_POLL_STAGE" 0 0 0 0 0 0 0)")" dpdk port)"
check "a DPDK dump without the client poll loop fails, naming both missing" \
    "1 FAILED: 4 stages; no client poll loop stage; no replication poll loop stage" \
    "$(status judge_trace "$(dump d "${PIPELINE_STAGES[@]}")" dpdk queue)"
check "a kernel dump needs no poll loop" "0 ok: 4 stages" \
    "$(status judge_trace "$(dump d "${PIPELINE_STAGES[@]}")" kernel "")"
check "an untraced primary's dump fails" \
    "1 FAILED: the primary was built without latency-trace" \
    "$(status judge_trace "$(dump d '# latency-trace disabled')" kernel "")"
check "a dump before any stage registered fails" \
    "1 FAILED: no stage in the /stats-dump of the primary" \
    "$(status judge_trace "$(dump d '# no samples')" dpdk shared)"
check "no answer from the primary fails" \
    "1 FAILED: no /stats-dump from the primary" \
    "$(status judge_trace "$(dump d)" dpdk shared)"
check "a truncated dump says so" "0 ok: 5 stages, 3 more cut off by the endpoint" \
    "$(status judge_trace "$(dump d "${PIPELINE_STAGES[@]}" "$CLIENT_POLL" $'# truncated\t3')" dpdk shared)"
check "a truncated dump that lost a poll loop fails" \
    "1 FAILED: 4 stages, 2 more cut off by the endpoint; no client poll loop stage" \
    "$(status judge_trace "$(dump d "${PIPELINE_STAGES[@]}" $'# truncated\t2')" dpdk shared)"
check "a stage line cut short is not a stage" \
    "1 FAILED: 4 stages; no client poll loop stage" \
    "$(status judge_trace "$(dump d "${PIPELINE_STAGES[@]}" $'stage\tdpdk poll: outer iteration (work-iterations only)\t98000\t210')" dpdk shared)"

echo ""
echo "=== trace_summary ==="
mkdir -p "${TMP_ROOT}/run"
SUMMARY_HEADER="run                          loop           iterations    p50 ns    p90 ns    p99 ns  p99.9 ns    max ns"
dump run/dpdk-queue-throughput.stats "${PIPELINE_STAGES[@]}" "$REPL_POLL" "$CLIENT_POLL" >/dev/null
dump run/dpdk-shared-throughput.stats "${PIPELINE_STAGES[@]}" "$CLIENT_POLL" >/dev/null
dump run/kernel-throughput.stats "${PIPELINE_STAGES[@]}" >/dev/null
dump run/dpdk-port-single.stats >/dev/null
check "the poll loops side by side, client first, a run without them said so" \
    "${SUMMARY_HEADER}
dpdk-queue-throughput        client poll         98000       640      1500      4100      9800     52000
dpdk-queue-throughput        repl poll           41000       520      1100      2600      6100     30000
dpdk-shared-throughput       client poll         98000       640      1500      4100      9800     52000
kernel-throughput            (no DPDK poll loop among its 4 stages)
dpdk-port-single             (no DPDK poll loop among its 0 stages)" \
    "$(trace_summary "${TMP_ROOT}/run/dpdk-queue-throughput.stats" "${TMP_ROOT}/run/dpdk-shared-throughput.stats" \
        "${TMP_ROOT}/run/kernel-throughput.stats" "${TMP_ROOT}/run/dpdk-port-single.stats")"

echo ""
echo "=== the trace stages read ==="
# The summary and the verdict find the poll loops by the names the node
# registers them under.
RUNTIME_SRC="${SCRIPT_DIR}/../../crates/core/server-runtime/src"
check "the client poll loop's stage is the node's" "1" \
    "$(grep -cF "\"${TRACE_CLIENT_POLL_STAGE}\"" "${RUNTIME_SRC}/dpdk_transport.rs")"
check "the replication poll loop's stage is the node's" "1" \
    "$(grep -cF "\"${TRACE_REPL_POLL_STAGE}\"" "${RUNTIME_SRC}/dpdk_repl_poll.rs")"
# And the dump's line as the health endpoint formats it: tab-separated,
# nine fields, beginning `stage`.
HEALTH_RS="${SCRIPT_DIR}/../../crates/core/transport-core/src/health.rs"
check "the dump's line format is the endpoint's" "1" \
    "$(grep -cF '"stage\t{name}\t{samples}\t{min}\t{p50}\t{p90}\t{p99}\t{p99_9}\t{max}\n"' "$HEALTH_RS")"
# And its markers, which judge_trace tells apart.
for marker in '"# latency-trace disabled"' '"# no samples"' '"# truncated\t{}"'; do
    check "the endpoint writes ${marker}" "1" "$(grep -cF "writeln!(c, ${marker}" "$HEALTH_RS")"
done


echo ""
echo "=== ${PASSED} passed, ${FAILED} failed ==="
[[ "$FAILED" -eq 0 ]]
