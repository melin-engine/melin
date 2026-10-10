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
