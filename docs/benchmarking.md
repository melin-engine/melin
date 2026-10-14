# Benchmarking across a LAN

`scripts/bench/lan-bench-suite.sh` measures a Melin application the way a
client sees it: requests cross a real network to a primary, which
journals and replicates each one before the reply comes back. It drives
the echo example (the sequencer's floor: an application that does
nothing), the notary example, or the Melin Exchange Core, on a primary
and two synchronous replicas, over kernel TCP or DPDK.

## Hosts

Four x86_64 machines on one private network:

| Role    | Runs                                     |
|---------|------------------------------------------|
| server  | the primary                              |
| bench   | the load generator                       |
| replica, replica2 | the replicas                   |

Each needs root over SSH (or a user with passwordless `sudo`), the same
Linux distribution and release, and a dedicated NVMe drive for the
journal if one is available. Prepare each once:

```sh
./scripts/bench/server-deploy.sh root@<host>
```

This installs the runtime packages, isolates every core but core 0 from
the scheduler, timers and RCU callbacks (`isolcpus`, `nohz_full`,
`rcu_nocbs`), disables SMT, deep C-states, transparent huge pages and the
lockup and clocksource watchdogs, forces the `performance` governor, pins
device interrupts to core 0, formats the journal drive as XFS at
`/mnt/journal`, and reserves hugepages for DPDK. The kernel parameters
go in a GRUB drop-in, `/etc/default/grub.d/99-melin-ec-bench.cfg`, and
take effect after the reboot the script performs; re-running the script
verifies that every one reached the running kernel. The drop-in is
rewritten on every run, so keep tuning of your own in a drop-in of
another name. Set `JOURNAL_DISK=/dev/nvmeXn1` to choose the journal
drive yourself.

For the DPDK transport, each host also needs `/etc/melin-dpdk.conf`,
written by `scripts/dpdk/dpdk-setup.sh`. The suite runs it on a host
that has none, which is enough for SR-IOV on Intel NICs. A Mellanox
(mlx5) NIC needs the address given, so run it on the host yourself:
`DPDK_IP=10.0.0.101/24 ./dpdk-setup.sh [--vlan <id>]`.

## The machine running the suite

The suite builds every binary where it runs, for the hosts' CPU, and
copies them over, so the bench hosts need no compiler. It reads the
CPU from the hosts (set `TARGET_CPU` to override) and refuses hosts that
differ, as one build cannot be tuned for two. It links against a sysroot
taken from the server host over SSH the first time and cached under
`~/.cache/melin-bench/`, so the binaries use exactly the glibc and libdpdk
the hosts run.

Locally it needs `rustup`, `clang`, `lld` and `python3`. The machine may
be of another architecture (an ARM laptop, say): the build is a cross
build either way. For the Exchange Core, a checkout at
`../exchange-core` beside this one (or `EC_REPO`).

## Running it

```sh
./scripts/bench/lan-bench-suite.sh <echo|notary|ec> <server-pub-ip> <bench-pub-ip> <server-private-ip> \
    <replica-pub-ip> <replica2-pub-ip>
```

The hosts are reached as root; set `SSH_USER` for another user.
`TRANSPORTS` picks from `kernel` and `dpdk` (comma-separated, or `all`;
`kernel` by default): clients and replication both on kernel TCP, or
both on DPDK. `WORKLOADS` picks from `throughput` (`THROUGHPUT_CLIENTS`
connections, each keeping `THROUGHPUT_WINDOW` requests in flight; 4 × 56
by default) and `single` (one request in flight). The script's header
lists every setting.

On DPDK, `DPDK_REPLICATION` picks where the nodes run replication, from
`shared` and `queue` (comma-separated, or `all`; `shared` by default),
each a pass of the DPDK transport of its own, from the same build:

- `shared`: on the client queue and its poll thread, as a DPDK node
  does without a replication flag.
- `queue`: on a second queue of the client NIC, served by a poll thread
  of its own (`--dpdk-replication-queue` on the primary and the
  replicas), the NIC steering replication to it. It needs a Mellanox
  (mlx5) NIC on the primary and both replicas: an SR-IOV virtual
  function cannot steer, and the suite refuses to start when a node's
  `/etc/melin-dpdk.conf` says SR-IOV, or when a node has none (the setup
  the suite runs on its own can only configure SR-IOV, and an mlx5 NIC
  needs its address given: run `scripts/dpdk/dpdk-setup.sh` with
  `DPDK_IP` on it first), naming the host. The primary's
  replication poll thread takes a core of its own, `repl-poll` in the
  node's `--cores` layout.

Replication on a NIC of its own (`port` mode) is not run by the suite
yet, and is refused.

Each run starts a fresh cluster on empty journals, acknowledging under
`ACK_POLICY` (`disk+ram` by default). The load generator warms up,
measures, then keeps the load on through a cool-down so that the last
measured replies arrive under the same load as the first. On the DPDK
transport it runs on DPDK too.

The load starts once both replicas are connected and none is catching
up. A replica that has only just caught up can still be busy with what
it received, and full load arriving within a second or two of that can
evict it; at the default replication ring size it then cannot rejoin
under that load, and the run is degraded. `SETTLE_SECS` waits that many
seconds between the replicas going live and the load starting: 5 is
enough on current hardware. The default, 0, starts the load straight
away, which is what a cluster taking traffic as soon as its replicas are
live would face.

`PREBUILT_BIN_DIR` names a local directory whose binaries replace the
suite's own builds of the same name, to compare a build made elsewhere
against the suite's.

## Rehearsing without hosts

`scripts/bench/test-containers.sh [echo|notary|ec]` runs the suite end to
end against four podman containers on the machine you run it from, over
kernel TCP, with short phases: the sysroot, the build, the deployment,
the cluster's start and the health verdict all run as they would on
bench hosts. The containers use your machine's architecture, so on an
ARM machine the build is for ARM rather than x86_64. The figures it
prints measure nothing, and it does not exercise DPDK.

## Results

Each run leaves, in `bench-results/lan-<app>-<timestamp>/`, under the
run's name (`kernel-<workload>` on kernel TCP, `dpdk-<mode>-<workload>`
on DPDK, so that the summary lists the replication modes side by side):

- `<run>.json`: throughput and latency percentiles, the shape every
  application's load generator writes;
- `<run>.txt`: the load generator's console output;
- `<run>.health`: the primary's replica metrics, sampled every second
  from the load-generating host;
- `<run>.queues` (DPDK only): the primary's packet counts per NIC port
  and queue, received and sent, sampled every second alike;
- `<run>.primary.log`, `.replica1.log` and `.replica2.log`: each node's
  log.

A latency is a full round trip as the client sees it, from the request
leaving the client to its reply arriving. A run whose replicas were
evicted, missing or catching up at any point is reported
as degraded, and the suite exits non-zero, as it does when a cluster
does not start or the load generator fails: a number measured on a
cluster that was not whole is not the number it claims to be.

In `queue` mode the run is also judged on its steering, from the
primary's packet counts: between the first and the last sample, queue 1
must have moved replication both ways (out to the replicas, their acks
back) and queue 0 the clients' requests and replies, each at least 100
packets per sampled second in each direction. That is well above what
an idle queue sees (heartbeats, ARP, stray frames) and well below what
any run makes it carry. A queue under it marks the run degraded, naming
the queue, as does a primary with no queue 1. The counts cannot tell
which connection a packet belonged to, so a connection steered to the
wrong queue while the right one carries the rest is caught by what it
causes instead: a replication link landing on the client queue is reset
and the replica drops out (the health verdict), and a client landing on
the replication queue is reset and the load generator fails. In `shared`
mode there is nothing to steer, and the counts are kept for reading
only.

After a run on SR-IOV the hosts are rebooted, to release the NIC's
virtual functions before the kernel transport uses it again; set
`SKIP_REBOOT=1` when chaining DPDK runs.
