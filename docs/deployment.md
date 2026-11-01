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

## Pipeline ring memory

Requests and replies pass between the node's stages through two rings,
each `--ring-capacity` slots (default 1048576). Every slot holds a whole
event or reply, so a ring's size is the capacity times the width of the
application's events (or replies) plus a fixed per-slot header, and
both are written in full when the node starts: that memory is resident,
and locked unless
`--no-mlock` is given, from boot, however lightly the node is loaded.
Replicas take the same `--ring-capacity`, and a replica promoted to
primary builds its primary rings at promotion at that capacity, so give
a replica the same ring memory as the primary it may replace.

Lower the capacity (a power of two, at least 1024) to fit a node of an
application with wide events into less memory. The cost is burst
headroom: the default absorbs about 100 ms of input at 10M events/s, and
a burst that outlasts a smaller ring is answered busy rather than
queued. `melin_input_queue_depth` against `melin_input_queue_capacity`
shows how much of it a node actually uses.

## A trusted network for DPDK

On DPDK a node runs its own TCP stack, which lacks the kernel's defence
against a flood of connection attempts (SYN cookies). Each port keeps a
few sockets ready for incoming connections, and an attempt holds one from
its first packet until its handshake completes or, if the peer never
answers, for 5 seconds. A host that keeps sending connection attempts and
never completes them, a few a second, can hold every one of them, and the
port refuses legitimate clients and replicas for as long as it does.
Client authentication does not help: it starts after the handshake.

So give a DPDK node's client and replication ports a network that only
trusted hosts can reach: a dedicated VLAN or private subnet, or a firewall
in front of the node that drops traffic from anywhere else. A node on
kernel TCP has the kernel's defences and does not need this.
