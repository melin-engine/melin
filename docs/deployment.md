# Deployment

What a node needs from the host it runs on, beyond the flags in
[Pipeline Architecture](pipeline-architecture.md#configuration).

## Locked memory

A node locks all of its memory into RAM at startup, so that a page fault
or a swap-in can never stall the pipeline mid-request. Locking needs
either the `CAP_IPC_LOCK` capability or a locked-memory limit
(`RLIMIT_MEMLOCK`) larger than everything the node will ever map. When
locking fails the node starts anyway, warns, and runs with its memory
unlocked.

The default limit is small. systemd gives every service, and every login
session, 8 MiB unless told otherwise — far less than a node maps. Raise it
in the node's service unit:

```ini
[Service]
LimitMEMLOCK=infinity
```

or grant the capability instead (`AmbientCapabilities=CAP_IPC_LOCK`). Under
a container runtime, the equivalent is the `memlock` ulimit
(`--ulimit memlock=-1:-1` for Docker) or the `IPC_LOCK` capability.

For development, `--no-mlock` skips locking altogether.

### The I/O rings count against the same limit

On the kernel TCP transport, the node's network I/O runs over Linux
io_uring, and the kernel charges io_uring's ring memory against the same
locked-memory limit — whether or not the node locks its memory, per user,
and across every process that user runs. Several nodes, or other
io_uring users, under one account share one budget, and a ring released
by a stopped process keeps counting for a moment after it exits.

The rings are sized from `--max-connections`: a node that serves a few
clients needs a few pages, one at the default cap needs a sizeable share
of an 8 MiB limit. A node that cannot create its rings refuses to start,
with an error naming `RLIMIT_MEMLOCK` and `LimitMEMLOCK=`. The fix is the
one above: raise the limit for the node's service, or grant
`CAP_IPC_LOCK`, which exempts the rings from the limit as well
(`--no-mlock` changes neither). Short of that, lower `--max-connections`
to the clients the node actually serves.

## DPDK replication on its own queue or NIC

What a DPDK node whose replication runs on a queue or a NIC of its own
(queue or port mode, see [Replication on its own queue or
NIC](replication.md#replication-on-its-own-queue-or-nic-dpdk)) needs
from its host, and what it gives up. A node in the default shared mode
needs none of this beyond a valid `repl-poll` entry in `--cores` (see
below).

### A core for the replication poll thread

The replication poll thread polls its queue flat out, whether or not it
has work, as the client poll thread does. `repl-poll` in `--cores`
places it, on core 5 by default. It runs on a node with
`--replication-bind`: on a primary from boot, on a replica from its
promotion. Both are held to it at boot, so a layout that a failover
candidate could not run as a primary is refused before it ever serves.
Give it an isolated core of its own, as the `reader`: a `y` on its entry
and a core shared with any other thread are refused, and an unpinned
`repl-poll=0` is accepted with a warning at boot, but still takes a full
core wherever the scheduler puts it. On a node that does not run it the
entry is checked as any unused thread's: `repl-poll=0` or a `y` on a
shared core is fine, but a busy-spinning `repl-poll` sharing a core with
another thread is refused there too. Every `--cores` value names it,
whatever the mode (see [Waiting](pipeline-architecture.md#waiting)).

### The NIC

- **Queue mode** needs a NIC with two queue pairs and flow steering: a
  Mellanox (mlx5) NIC, dedicated or bifurcated. SR-IOV virtual functions
  are not supported. On a dedicated port, the node points the NIC's RSS
  table at the client queue when the port starts (logged as `RSS
  redirection table pinned to one queue`), so that whatever no
  replication rule takes goes to the clients' queue.
- **Port mode** needs a second port on the same network, with an address
  of its own in the first port's subnet, and EAL arguments that allow
  both devices (`--dpdk-eal-args="-a <client-device> -a
  <replication-device>"`). DPDK numbers the ports in the order it finds
  the devices, by PCI address for PCI devices rather than in the order
  of the `-a` arguments, so the client NIC is not always port 0. On an
  mlx5 host, `scripts/dpdk/dpdk-setup.sh` sets up both ports and records
  their port ids (see [A second NIC for port
  mode](benchmarking.md#a-second-nic-for-port-mode)).
- **On AWS**, port mode takes a second ENI in the same subnet as the
  first, bound to DPDK beside it. ENA Express is set per ENI: enable it
  on the replication ENI too. The instance's bandwidth and packet-rate
  allowances are shared by all its ENIs, so a second ENI separates
  queues and cores, not that budget.

### DPDK's multi-process tools cannot attach

The replication poll thread registers with DPDK as a thread of its own,
which gives it a per-thread buffer cache, and DPDK turns its
multi-process support off for a process that does this. Once the thread
runs (on a primary from boot, on a replica from its promotion), tools
that attach to the node as a secondary DPDK process, such as
`dpdk-proc-info`, `dpdk-dumpcap` and `dpdk-pdump`, cannot attach to it.
A replica promoted while such a tool is attached runs the thread without
the cache, with a warning in the log (`replication poll thread not
registered with EAL`). The per-queue counters below take the place of
`dpdk-proc-info` for checking where traffic goes.

### Checking where the traffic goes

A DPDK primary exports, on its health endpoint's `/metrics`
(`--health-bind`), the packets its poll threads took off each NIC queue
and put on it, labelled by DPDK port and queue:
`melin_dpdk_queue_rx_packets_total` and
`melin_dpdk_queue_tx_packets_total`. In queue mode, replication should
move on queue 1 of the client NIC's ports and client traffic on queue 0;
in port mode, replication on the replication NIC's ports. Check them
right after a node starts serving. They count what the node handled, not
what the NIC dropped before a queue: for that, read the NIC's own
statistics. A replication packet the NIC steers to the client queue is
answered with a reset, so a replica whose link keeps dropping and
reconnecting in queue mode is the symptom to look for.
