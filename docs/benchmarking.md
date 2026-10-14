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

Each run starts a fresh cluster on empty journals, acknowledging under
`ACK_POLICY` (`disk+ram` by default). The load generator warms up,
measures, then keeps the load on through a cool-down so that the last
measured replies arrive under the same load as the first. On the DPDK
transport it runs on DPDK too.

## Results

Each run leaves, in `bench-results/lan-<app>-<timestamp>/`:

- `<transport>-<workload>.json`: throughput and latency percentiles, the
  shape every application's load generator writes;
- `<transport>-<workload>.txt`: the load generator's console output;
- `<transport>-<workload>.health`: the primary's replica metrics,
  sampled every second;
- `<transport>-<workload>.primary.log`: the primary's log.

A latency is a full round trip as the client sees it, from the request
leaving the client to its reply arriving. A run whose replicas were
evicted, missing or catching up at any point is reported
as degraded, and the suite exits non-zero, as it does when a cluster
does not start or the load generator fails: a number measured on a
cluster that was not whole is not the number it claims to be.

After a run on SR-IOV the hosts are rebooted, to release the NIC's
virtual functions before the kernel transport uses it again; set
`SKIP_REBOOT=1` when chaining DPDK runs.
