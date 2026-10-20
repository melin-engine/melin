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
another name.

The journal goes on the largest unpartitioned NVMe drive. Drives of
the same size are told apart by serial number, because the kernel's
names for them (`nvme2n1`, `nvme3n1`) can change from one boot to the
next. Once the journal is set up, a re-run keeps it on the drive that
holds its filesystem, whatever that drive is called now. To choose the
drive yourself, set `JOURNAL_DISK`, preferably to a name that does not
change, such as `/dev/disk/by-id/nvme-<model>_<serial>`. The script
refuses to put the journal on another drive than the one `/etc/fstab`
already mounts it from, and refuses to format a replacement when that
drive is missing: to move the journal, unmount it and remove its line
from `/etc/fstab` first.

The nodes write their snapshots to the root filesystem (see "Snapshots"
below), so on the server and replica hosts the root filesystem must be
on a disk other than the journal drive. A host with no journal drive of
its own, whose `/mnt/journal` is then on the root disk, can run the suite
only with snapshots off.

For the DPDK transport, each host also needs `/etc/melin-dpdk.conf`,
written by `scripts/dpdk/dpdk-setup.sh`. The suite runs it on a host
that has none, which is enough for SR-IOV on Intel and Broadcom NICs:
behind an LACP bond, it creates a virtual function on each member; on a
plain port, it creates one on the first port that does not carry the
default route, joins the VLAN of the kernel's VLAN interface on that
port (`eno2.2142`, say), and takes the address 100 above the kernel's
there. SR-IOV must be enabled in the host's firmware, and the IOMMU on.
A Mellanox (mlx5) NIC needs the address given, so run it on the host
yourself: `DPDK_IP=10.0.0.101/24 ./dpdk-setup.sh [--vlan <id>]`.

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

The load starts once both replicas are connected and none is catching
up. A replica that has only just caught up can still be busy with what
it received, and full load arriving within a second or two of that can
evict it; at the default replication ring size it then cannot rejoin
under that load, and the run is unsteady. `SETTLE_SECS` waits that many
seconds between the replicas going live and the load starting, 5 by
default, which is enough on current hardware. `SETTLE_SECS=0` starts the
load straight away, which is what a cluster taking traffic as soon as its
replicas are live would face; the Exchange Core evicts a replica under it
on current builds.

`PREBUILT_BIN_DIR` names a local directory whose binaries replace the
suite's own builds of the same name, to compare a build made elsewhere
against the suite's.

### Snapshots

Every node takes snapshots throughout every run, as it does in
production, where they are on by default. Each node saves one every
`SNAPSHOT_INTERVAL_MS` (10 s by default) to `<node>.snapshot` in
`SNAPSHOT_DIR` (`/var/lib/melin-bench/snapshots` by default, on the root
filesystem). The interval is far shorter than a production one, so that
several saves land inside every measured phase: the figures show what
snapshots cost the latency at worst, not on average. The interval must
be at least 1000 ms and no longer than the shortest measured phase
requested; `0` turns snapshots off, to tell whether a regression comes
from them. The suite sets each node's snapshot flags itself and refuses
`SERVER_EXTRA_ARGS` or `REPLICA_EXTRA_ARGS` that carry one.

The snapshots are kept on another disk than the journal, which is the
layout [the journal documentation](journal.md#where-snapshots-go)
recommends for latency-critical deployments: a save then shares no
device queue and no filesystem with the journal's writes and syncs.
Before the first run the suite resolves `SNAPSHOT_DIR` and `JOURNAL_DIR`
on each node host to the disks under them, through partitions, LVM and
software RAID, and refuses to run if they share a disk, if
`SNAPSHOT_DIR` is on a filesystem held in memory (such as `tmpfs`), or if
either cannot be resolved to a disk at all: a run that cannot show the
separation does not claim it. With snapshots off the check is skipped.

Before each run, every node's journal and snapshot files from earlier
runs are removed, by their exact names, so a run never starts from
another's state, and an aborted run leaves nothing behind for the next
one to read. The last run's files are left in place for inspection.
`SNAPSHOT_DIR` may be a directory shared with other files: nothing else
in it is touched. Files left by an aborted run under a different
`SNAPSHOT_DIR` are not removed; no node reads them, but they take space.

## Rehearsing without hosts

`scripts/bench/test-containers.sh [echo|notary|ec]` runs the suite end to
end against four podman containers on the machine you run it from, over
kernel TCP, with short phases: the sysroot, the build, the deployment,
the cluster's start and the health verdict all run as they would on
bench hosts. The containers use your machine's architecture, so on an
ARM machine the build is for ARM rather than x86_64. The figures it
prints measure nothing, and it does not exercise DPDK.

The rehearsal takes a snapshot every 2 s, so that each of its short
phases is judged on at least one save per node. A container's filesystem
cannot be resolved to a disk, so the rehearsal sets
`SNAPSHOT_SHARED_DISK_OK=1`, which turns the disk check's refusal into a
warning and records the disks' writes as `-`. That setting is for
rehearsals only: a run made with it does not show that its snapshots
were kept off the journal's disk.

## Results

Each run leaves, in `bench-results/lan-<app>-<timestamp>/`:

- `<transport>-<workload>.json`: throughput and latency percentiles, the
  shape every application's load generator writes;
- `<transport>-<workload>.txt`: the load generator's console output;
- `<transport>-<workload>.health`: the primary's replica metrics,
  sampled every second from the load-generating host;
- `<transport>-<workload>.primary.log`, `.replica1.log` and
  `.replica2.log`: each node's log;
- `<transport>-<workload>.snapshots`, with snapshots on: each node's
  snapshot verdict (the saves it made during the load, how many it
  needed, the slowest and largest of them, and the most attempts any of
  them took: a save is retried until the node can take it consistently,
  so a count above 1 under heavy load explains a node that fell short of
  its saves), then the bytes written
  to each node's snapshot disk and journal disk over the run;
- `suite.conf`: every setting the suite ran with, the revision it was
  built from, the CPU it was built for, and the disks each node's
  snapshots and journal were found on.

A latency is a full round trip as the client sees it, from the request
leaving the client to its reply arriving. A request the node shed under
load (answered busy) is counted as shed by the echo and notary load
generator, in its console output and in the JSON (`shed_requests`),
rather than failing the run: a run that shed any measured a node past
its capacity. A run whose replicas were
evicted, missing or catching up at any point is reported
as `UNSTEADY`, and the suite exits non-zero, as it does when a cluster
does not start or the load generator fails: a number measured on a
cluster that was not whole is not the number it claims to be.

A run does not pass, either, unless every node snapshotted during it.
Each node must have saved at least one snapshot for every interval of
the measured phase but one (and at least one in all), to its own path in
`SNAPSHOT_DIR`; a failed save, or a save anywhere else, fails the run
however many landed. A run that took no snapshots measured a
configuration nobody deploys. The summary table gives, for each run, the
fewest saves any node made during the load and the slowest save, the
figure to compare with the run's latency outliers; both are `-` with
snapshots off.

After a run on SR-IOV the hosts are rebooted, to release the NIC's
virtual functions before the kernel transport uses it again; set
`SKIP_REBOOT=1` when chaining DPDK runs. The next DPDK run sets the
virtual functions up again, with the addresses it used before.
