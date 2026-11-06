# Compatibility

What an upgrade of Melin may change, what it never changes, and how a release tells you which. It covers the data a node keeps on disk, the protocols between nodes and between clients and nodes, the interface an operator scripts against, and the Rust crates an application is built on.

The policy has two tiers. The rules marked **now** hold for every release from this one on, while Melin is at `0.x`. The rules marked **from 1.0** are what the first stable release commits to; until then they describe where the project is going, and the gaps between the two are listed at the end.

## The rule that holds everywhere

**Refuse, never guess.** Every format and protocol that crosses a version boundary carries its own version. A node or client that meets one it cannot read refuses it, with an error naming both versions, before it acts on anything: before it replays an entry, applies a frame, or signs a challenge. A release that reads data or a frame as something other than what was written is broken, whatever else it gets right, and a fix for it takes precedence over any other work.

The rest of this document is about how much a release can read. This rule is about what happens at the edge of that: an explicit stop, never a different history.

## Versions

**The release version.** Every published crate shares one version number, and it follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html):

- **A patch release** (`0.20.0` → `0.20.1`) fixes bugs. It changes no format, no protocol, no flag, no metric and no public Rust item. Nodes can be moved to it one at a time, and it can be rolled back the same way.
- **A minor release** (`0.20` → `0.21`) adds features. **Now**, it may also break any surface below, and says so (see [What every release states](#what-every-release-states)). **From 1.0**, it breaks nothing this document promises.
- **A major release** (`1.x` → `2.0`, from 1.0) is the only release that may break a promise this document makes.

**The surface versions.** Beside the release version, each format and protocol has its own number, and it changes only when that surface does: the journal format, the snapshot framing, the replication protocol, the client protocol, and the control plane's state directory. Most releases change none of them. The release notes name each one that moved. Their histories are in [journal](journal.md#format-versioning) and [replication](replication.md#upgrade-and-rollback-notes).

**Your application's versions are yours.** The bytes of your events, your requests and responses, and your snapshot are encoded by your application. Melin carries them without reading them, and their compatibility is your application's to keep; see [Your application's own compatibility](#your-applications-own-compatibility).

## Data at rest

The journal, the snapshots and the control plane's state directory. These hold the history an operator is accountable for, and get the strongest promises.

**Reading older data.**

- **Now:** a release reads the journal, snapshots and control-plane state written by the release before it, so an upgrade restarts every node on its existing files. When a release cannot, its release notes say so and give the procedure, which is a snapshot boundary (see [Migration](journal.md#migration-procedure)).
- **From 1.0:** a release reads all the data written by every earlier release of the same major version. A format a later release cannot read takes a new major version and a documented migration.

**Reading newer data.** A release refuses data a newer release wrote, by its version, and changes nothing on disk. It does not try to read what it can make sense of.

**Rolling back.**

- **Now:** rolling back across a release that changed a format needs a snapshot written by the older release, or a resync of the node as a fresh replica of a node already running the older release. The release notes say when this applies.
- **From 1.0:** a format change is made in two steps. One release learns to read the new format, and a later one starts writing it. Rolling back by one minor release is then always possible on the existing files.

**Replaying for audit.** A journal can always be replayed by the release that wrote it: archive the binary with the journal. **From 1.0**, every later release of the same major version can replay it too.

**Replay reproduces what the application saw.** A release that replays a history hands your application the same events, in the same order, with the same timestamps and the same time-driven calls as the release that wrote it. When a release changes what an application is handed, such as how sequencer time advances or when ticks fall, the change is recorded with the history, so an existing history keeps replaying under the rules it was written under. A newer release replaying an older journal arrives at the same state, and the audit trail reproduces.

## Between nodes

The replication protocol, which carries the history from the primary to its replicas, and the control plane's messages, which elect and fence the primary.

- **Now:** every node of a cluster runs the same minor release. A primary and a replica on different replication protocol versions refuse each other at the handshake, in either direction, so replication stops, and with it writes under an ack policy that needs a replica. Nothing is corrupted. A minor upgrade stops the whole cluster, deploys the new release everywhere and restarts it, in one maintenance window. A patch release changes neither protocol, so nodes can take one a node at a time.
- **From 1.0:** a rolling upgrade across adjacent minor releases. A node on release N+1 speaks release N's protocols to a peer still on N, agreed at the handshake, so each node of a cluster can be upgraded in turn while the rest keep serving. Upgrading by more than one minor release at a time still stops the whole cluster.

## Between clients and nodes

The client protocol: framing, authentication, request identifiers and the reply batch.

- **Now:** a client and a node must speak the same client protocol version. The client checks it while connecting, before its key signs anything, and reports the mismatch. A release that changes the client protocol says so, and its clients upgrade with its nodes. A patch release never changes it.
- **From 1.0:** a node also accepts clients of the previous client protocol version, until the next major release. The nodes upgrade first, and a fleet of clients follows at its own pace. A client never needs to upgrade before the nodes it talks to.

The requests and responses inside the protocol are your application's encoding (see below).

## The operator interface

Command-line flags, admin commands, the `/healthz` fields, the `/metrics` series names and labels, exit codes, and the layout of a node's data directory.

- **Additions are not breaking.** A release may add a flag, an admin command, a health field, a metric or a label value. Scripts, probes and dashboards should tolerate fields and series they do not know.
- **Removals and renames are deprecated first.** The old spelling keeps working for at least one minor release beside the new one, logs a warning each time it is used, and is listed under **Deprecated** in the release notes. It is removed in a later minor release (**now**) or the next major one (**from 1.0**), and the release that removes it lists it under **Removed**.
- **A changed default is a breaking change.** A new default for a flag (an ack policy, a connection cap, a timeout) is listed under **Changed**, with the flag that keeps the old behaviour.
- **A changed meaning is a breaking change.** A metric that keeps its name but counts something else, or a flag that keeps its name but does something else, is listed under **Changed**. From 1.0 it takes a new name instead.

Not part of the interface, and free to change in any release:

- **Log messages.** Their text and their level are for people reading them. Alert on metrics and on the health endpoint, not on log text.
- **`--help` output**, beyond the flags it lists.
- **Diagnostic build features** (`no-persist`, `pipeline-stats`, `latency-trace`) and what they print.
- **The benchmark and test tools** in the repository, and the example applications. They are not published.

## The Rust crates

The crates an application is built on (`melin-app`, `melin-server-runtime` and the crates beneath it) and the client library a client links (`melin-client`).

- **Semantic Versioning as Cargo reads it.** A change that can stop a dependent from compiling is a breaking change. **Now**, it may come in a minor release, listed under **Changed** or **Removed**. **From 1.0**, it waits for a major release. A patch release never contains one.
- **Every enum and public struct is exhaustive.** None is marked `#[non_exhaustive]`, so adding a variant or a public field is a breaking change, and your code fails to compile until it decides what to do with the new case, rather than letting it fall into a wildcard arm. This is deliberate: the compiler finds each place a new case needs a decision.
- **The minimum supported Rust version** is in each crate's manifest. Raising it takes a minor release, never a patch, and is listed under **Changed**.
- **Cargo features** are part of the API. A feature is not renamed or removed outside a breaking release. A feature that no longer does anything stays as a no-op until then.
- **The unpublished crates** (the examples, the test harness, the benchmark and the shared-memory proxy) make no promise.

## Your application's own compatibility

Melin checks the version your application gives its snapshot layout, and refuses a snapshot written under another. It records no version for your event encoding: an event journaled by one version of your application is decoded by whichever version replays it. Adding a new kind of event is safe to replay, once every node runs a version that knows it. Changing the layout of an existing one is safe only across a snapshot boundary. The rules and the procedures are in [Snapshots and upgrades](building-an-application.md#snapshots-and-upgrades) and [Changing the application's encoding](journal.md#changing-the-applications-encoding).

A client built for one version of your application, sending a request the node's version does not understand, is refused by your decoder, and counted on `/metrics` (see [Observability](replication.md#observability)).

## Support

- **Now:** fixes land in the next release. Earlier releases are not patched; upgrade to take a fix.
- **From 1.0:** the latest minor release takes every fix. Fixes for correctness and security go to the previous minor release too.

## What every release states

Each release's notes in the [changelog](../CHANGELOG.md) list:

- every breaking change, under **Changed** or **Removed**, with what to do about it;
- every deprecation, under **Deprecated**, with its replacement;
- every surface version that moved (journal format, snapshot framing, replication protocol, client protocol, control-plane state) and what that means for an upgrade: in place on the existing files, across a snapshot boundary, whole cluster at once, clients with the nodes;
- what a rollback from it takes, when it is more than reinstalling the previous release.

A release whose notes say none of these upgrades in place, one node at a time, and rolls back the same way.

## Where this release falls short

Gaps between the policy above and the release you are running. Each is a bug against this document, and is removed from this list by the release that fixes it.

- **Control-plane messages carry no version.** Nodes on different releases with the control plane enabled are not refused when they connect, and may misread each other's messages. Never run the control plane across releases: upgrade the whole cluster at once, as the replication protocol already requires.
- **The event encoding is not versioned** (see [Your application's own compatibility](#your-applications-own-compatibility)). A change to an event's layout that still decodes the old bytes replays a different history without an error. Only the procedure guards against it today.
- **The `DURABILITY` admin command** is still accepted, though its deprecation said it would be removed in the minor release after it was deprecated. Its removal is overdue; until then, it keeps working and logs a warning.
