//! End-to-end contract for the tick watermark: a copy of the application
//! that was restarted, promoted or restored makes the same
//! `Application::tick` calls as a copy that ran uninterrupted over the
//! same journal.
//!
//! The application counts every tick call, a repeated time included —
//! the idempotence rule every real application keeps, broken on purpose
//! (`TickCountingApp`) — and its reports and queries carry the count. A
//! copy that made one tick call more or fewer reports a different count
//! from then on.
//!
//! The reference every case compares against is a replay of the node's
//! own journal from its first entry, after the node has stopped: one copy
//! that applied the whole history without a break, through the same
//! dispatch step every copy uses. A node answers a query with its count
//! and the journal sequence it is at; the replay must reach the same
//! count at that sequence.
//!
//! A break only shows where it falls between two entries stamped with the
//! same time, or before entries stamped behind the time the copy had
//! reached: a copy that restarted its watermark at zero there makes a
//! tick call the uninterrupted one does not. The cases driven by a real
//! primary under load repeat until the break fell there, as the journal
//! shows; the cases driven by a scripted primary build that history
//! outright, stamped ahead of this host's clock, which also stands in for
//! an old primary whose clock ran ahead of the promoted node's.
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see `docs/internal/dpdk-testing.md`).
//! The scripted primary is a kernel socket on `melin_test_node::local_ip`,
//! which the nodes reach on either transport.

#[path = "common/halt_cluster.rs"]
mod halt_cluster;
#[path = "common/scripted_primary.rs"]
mod scripted_primary;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use melin_app::{Application, RejectReason};
use melin_client::{Connection, Frame};
use melin_journal::test_utils::PREALLOC;
use melin_journal::{BufferedWriter, JournalReader};
use melin_server_runtime::ack_policy::AckPolicy;
use melin_test_node::probe::{
    PROBE_LIMIT, admin_reply_when_up, admin_until_ok, gauge, wait_for_gauge, wait_for_gauge_where,
    wait_for_snapshot_where, wait_released,
};
use melin_test_node::{Addrs, Node};
use melin_transport_core::JournaledApp;
use melin_transport_core::test_support::{
    TestEvent, TestQuery, TickCountingApp, TickCountingCodec,
};

use halt_cluster::{Fixture, addrs, endpoint, spawn_node};
use scripted_primary::ScriptedPrimary;

/// Requests a load connection sends before reading their replies: enough
/// that one receive on the node carries many, which it stamps with one
/// time and commits a few at a time, so runs of entries share a
/// timestamp and the journal's batches cut through them.
const BURST: usize = 64;

/// Connections of pipelined writes the load runs at once: the node's
/// reader stamps the receives of every connection it drains in one pass
/// with the same time, which lengthens the runs.
const LOAD_CONNECTIONS: usize = 8;

/// A node's tick count and journal sequence, as its query answers or a
/// replay of its journal reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Position {
    ticks: u64,
    sequence: u64,
}

/// The node's tick count and the journal sequence it answers it at.
fn query(fx: &Fixture, node: &Node) -> Position {
    let mut conn = fx.client(node);
    let body = conn
        .request_one(&TickCountingCodec::request(TestEvent::Query))
        .expect("the query is answered");
    let TestQuery {
        total,
        journal_sequence,
    } = TickCountingCodec::query(&body).expect("a query's body");
    Position {
        ticks: total,
        sequence: journal_sequence,
    }
}

/// The count an uninterrupted copy reaches over the journal at `journal`
/// (its archives included), and where the journal ends: a replay from
/// its first entry, on a copy of its files, so the node's own are left as
/// they are.
fn replayed(journal: &Path) -> Position {
    let copy = tempfile::tempdir().expect("tempdir");
    let target = |file: &Path| copy.path().join(file.file_name().expect("a file name"));
    let archives = melin_journal::segment::list_archives(journal).expect("list archives");
    for (_, archive) in &archives {
        std::fs::copy(archive, target(archive)).expect("copy an archive");
    }
    std::fs::copy(journal, target(journal)).expect("copy the journal");
    let replay = JournaledApp::<TickCountingApp, BufferedWriter<TestEvent>>::recover(
        TickCountingApp::default(),
        &target(journal),
        PREALLOC,
    )
    .expect("replay the journal");
    let ticks = replay.app().ticks;
    let (_, writer) = replay.into_parts();
    Position {
        ticks,
        sequence: writer.next_sequence() - 1,
    }
}

/// Every entry of the journal at `journal`, archives first, as its
/// sequence and timestamp, in order. `u64` both, as the journal records
/// them.
fn stamps(journal: &Path) -> Vec<(u64, u64)> {
    let mut stamps = Vec::new();
    let mut read = |mut reader: JournalReader<TestEvent>| {
        while let Some(entry) = reader.next_entry().expect("read an entry") {
            stamps.push((entry.sequence, entry.timestamp_ns));
        }
    };
    for (_, archive) in melin_journal::segment::list_archives(journal).expect("list archives") {
        read(JournalReader::open_archived(&archive).expect("open an archive"));
    }
    read(JournalReader::open(journal).expect("open the journal"));
    stamps
}

/// Whether the entries at `sequence` and the one after it are both in
/// `stamps` and share a timestamp: a copy that broke off after `sequence`
/// and restarted its watermark at zero would tick before the next entry
/// where the uninterrupted copy does not.
fn shares_time_with_next(stamps: &[(u64, u64)], sequence: u64) -> bool {
    let time_at = |s: u64| stamps.iter().find(|&&(seq, _)| seq == s).map(|&(_, ts)| ts);
    matches!((time_at(sequence), time_at(sequence + 1)), (Some(a), Some(b)) if a == b)
}

/// Pipelined writes on [`LOAD_CONNECTIONS`] connections, in bursts of
/// [`BURST`].
struct Load {
    stop: Arc<AtomicBool>,
    writers: Vec<JoinHandle<Written>>,
}

/// What one load connection saw: the tick count the report of each write
/// it got applied carries, in order (a write a halted node refused is
/// left out), and the error that ended it, if one did.
struct Written {
    reports: Vec<u64>,
    error: Option<String>,
}

impl Load {
    /// Start the writers on `node`: each sends `bursts` bursts, or, for
    /// `None`, bursts until [`Self::finish`] stops it.
    fn start(fx: &Fixture, node: &Node, bursts: Option<usize>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let writers = (0..LOAD_CONNECTIONS)
            .map(|_| {
                let mut conn = fx.client(node);
                conn.set_read_timeout(PROBE_LIMIT)
                    .expect("set the read timeout");
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    write_bursts(&mut conn, |sent| {
                        bursts.map_or(!stop.load(Ordering::Relaxed), |bursts| sent < bursts)
                    })
                })
            })
            .collect();
        Self { stop, writers }
    }

    /// Stop the writers (those sending until stopped) and return what
    /// they saw.
    fn finish(self) -> Vec<Written> {
        self.stop.store(true, Ordering::Relaxed);
        self.writers
            .into_iter()
            .map(|writer| writer.join().expect("writer thread"))
            .collect()
    }

    /// [`Self::finish`], for a node that served throughout: every writer
    /// ran without error. Returns each writer's counts.
    fn finish_served(self) -> Vec<Vec<u64>> {
        self.finish()
            .into_iter()
            .map(|written| {
                assert_eq!(
                    written.error,
                    None,
                    "a writer's stream ended after {} reports",
                    written.reports.len()
                );
                written.reports
            })
            .collect()
    }
}

/// Bursts of writes, each followed by its replies, for as long as
/// `more`, given the bursts sent so far, says.
///
/// A burst is sent before the replies to the one before it are read, so
/// the node always has one in hand: it never runs dry between bursts,
/// and where its batches end falls anywhere in the stream, not only
/// where a burst does.
fn write_bursts(conn: &mut Connection, more: impl Fn(usize) -> bool) -> Written {
    let add = TickCountingCodec::request(TestEvent::Add(1));
    let refusal =
        TickCountingApp::build_reject(&TestEvent::Add(1), RejectReason::ReplicaDisconnected);
    let mut reports = Vec::new();
    let mut sent = 0;
    // Requests sent and not yet answered. `usize`, as the burst size.
    let mut in_flight = 0;
    let failed = |reports, e: melin_client::Error| Written {
        reports,
        error: Some(e.to_string()),
    };
    loop {
        let sending = more(sent);
        if sending {
            for _ in 0..BURST {
                if let Err(e) = conn.send(&add) {
                    return failed(reports, e);
                }
            }
            sent += 1;
            in_flight += BURST;
        }
        // One burst left in flight while sending, none at the end.
        let keep = if sending { BURST } else { 0 };
        while in_flight > keep {
            match conn.next_frame() {
                Ok(Frame::Response(body)) => {
                    let report = TickCountingCodec::report(body).expect("a report's body");
                    // A write a halted node refused carries no count.
                    if report != refusal {
                        reports.push(report.ticks);
                    }
                }
                Ok(Frame::BatchEnd { .. } | Frame::ServerBusy { .. } | Frame::Refused { .. }) => {
                    in_flight -= 1;
                }
                Err(e) => return failed(reports, e),
            }
        }
        if !sending {
            return Written {
                reports,
                error: None,
            };
        }
    }
}

/// Wait, at most [`PROBE_LIMIT`], until the node behind `health` has
/// journaled `entries` more than it had: the load is running through it.
fn wait_for_progress(health: SocketAddr, entries: u64) {
    let from = gauge(health, "melin_journal_sequence").expect("the node's sequence");
    wait_for_gauge_where(
        health,
        "melin_journal_sequence",
        "advanced under the load",
        |v| v >= from + entries,
    );
}

/// Wait, at most [`PROBE_LIMIT`], until the sequence the primary's one
/// replica has acked as journaled is one `accept` takes. A replica's own
/// health endpoint reports no journal sequence, so the primary's view of
/// it is the one to read; a lone replica takes the first slot, however
/// often it reconnects.
fn wait_for_replica(health: SocketAddr, what: &str, accept: impl Fn(u64) -> bool) {
    wait_for_gauge_where(
        health,
        "melin_replica_acked_sequence{slot=\"0\"}",
        what,
        accept,
    );
}

/// A replica restarted under load recovers from its journal and goes on
/// from where it stopped, making the tick calls an uninterrupted copy
/// makes: the watermark replay reached seeds its matching stage.
///
/// A primary, its replica, and pipelined writes, under an ack policy that
/// holds each reply for the replica, so the writes run at the replica's
/// pace. The replica is stopped and restarted while the writes run: the
/// primary refuses writes while it is gone, and takes them again once it
/// is back and streaming, which it is while catching up on what it
/// missed. A replica takes no clients, so its count is read the way an
/// operator would come to read it: the primary is stopped, the replica
/// promoted, and its query compared with the replay of its own journal.
/// A promotion carries the replica's count and watermark over as they are
/// (the next test's subject), so a tick call the restart added shows
/// here.
///
/// A restart only matters where the replica's journal ended inside a run
/// of entries stamped with one time: the first entry it then applies
/// repeats its watermark. Where it ends is the load's doing, so the
/// scenario runs again, on a new cluster, until one restart fell there,
/// as the replica's journal shows afterwards. Each run restarts once: a
/// second restart would recover the first one's extra tick away, since
/// replay starts from the journal.
/// The runs are bounded to keep the test inside the suite's time limit,
/// so on a rare unlucky draw none falls there and the test checks only
/// what the runs exercised; the case itself is pinned, every time, by
/// [`a_replica_restarted_inside_a_run_of_one_timestamp_goes_on_from_its_watermark`].
#[test]
fn a_replica_restarted_under_load_ticks_as_an_uninterrupted_copy() {
    /// Runs at most, until a restart cuts a run of shared timestamps.
    const RUNS: usize = 12;

    melin_test_node::logs::capture_if_requested();
    // Every run's nodes at the same addresses, and the primary's health
    // and the replica's admin endpoints at the same ports: a test process
    // has only so many ports.
    let cluster = Cluster {
        primary: addrs(0),
        replica: addrs(1),
        health: endpoint(),
        admin: endpoint(),
    };
    // The journal end of each run's restart.
    let mut cuts = Vec::new();
    for _ in 0..RUNS {
        let (cut, shared) = restart_a_replica_under_load(&cluster);
        cuts.push(cut);
        if shared {
            return;
        }
    }
    eprintln!(
        "none of the {RUNS} restarts (journal ends {cuts:?}) fell inside a run of shared \
         timestamps; the deterministic twin covers that case"
    );
}

/// Where every run of [`a_replica_restarted_under_load_ticks_as_an_uninterrupted_copy`]
/// puts its primary and its replica.
struct Cluster {
    primary: Addrs,
    replica: Addrs,
    /// The primary's health endpoint.
    health: SocketAddr,
    /// The replica's admin endpoint, once restarted.
    admin: SocketAddr,
}

/// One run of [`a_replica_restarted_under_load_ticks_as_an_uninterrupted_copy`],
/// on a new cluster of a primary and a replica at `cluster`: checks the
/// restarted replica's count, and returns where its journal ended at the
/// restart and whether that cut a run of shared timestamps.
fn restart_a_replica_under_load(cluster: &Cluster) -> (u64, bool) {
    /// Entries the load journals before the restart.
    const BEFORE: u64 = 2_000;
    /// A reply waits for the replica to hold its write: the load then
    /// runs at the replica's pace, rather than outrunning it until the
    /// primary evicts it.
    const POLICY: AckPolicy = AckPolicy::DiskAndRam;

    let Cluster {
        primary: primary_addrs,
        replica: replica_addrs,
        health,
        admin,
    } = cluster;
    let (health, admin) = (*health, *admin);
    let fx = Fixture::new(0xB1);
    let mut primary_config = fx.primary_config(primary_addrs, POLICY);
    primary_config.health_bind = Some(health);
    // The last run's nodes may hold these ports a moment past their stop.
    wait_released(&[health, admin]);
    let primary = spawn_node::<TickCountingApp>(primary_addrs, primary_config);
    let replica_config = |admin| {
        let mut config = fx.replica_config("replica", primary_addrs.replication(), POLICY);
        config.admin_bind = admin;
        config
    };
    let replica_journal = replica_config(None).journal;
    let replica = spawn_node::<TickCountingApp>(replica_addrs, replica_config(None));
    wait_for_gauge(health, "melin_replicas_connected", 1);

    let load = Load::start(&fx, &primary, None);
    wait_for_progress(health, BEFORE);
    replica.stop();
    let cut = stamps(&replica_journal)
        .last()
        .map_or(0, |&(sequence, _)| sequence);
    let replica = spawn_node::<TickCountingApp>(replica_addrs, replica_config(Some(admin)));
    // Under load again, from past the cut.
    wait_for_replica(health, "passed the restart", |acked| acked > cut);
    wait_for_progress(health, BEFORE);
    let reports = load.finish_served();
    for reports in &reports {
        assert!(
            reports.windows(2).all(|pair| pair[0] <= pair[1]),
            "a connection's counts went backwards"
        );
    }

    // Caught up, then the only node left: promoted, and serving.
    let head = gauge(health, "melin_journal_sequence").expect("the primary's sequence");
    wait_for_replica(health, "caught up", |acked| acked == head);
    primary.stop();
    admin_reply_when_up(admin, &fx.client_key, "PROMOTE");
    // Its own disk is its only one: the operator consents to serving on
    // it.
    admin_until_ok(admin, &fx.client_key, "ACK-POLICY disk");
    // A write answered once its own journal holds it: the query behind it
    // then reads a position the journal holds, the operator's policy
    // change included.
    Load::start(&fx, &replica, Some(1)).finish_served();
    let served = query(&fx, &replica);
    replica.stop();

    let shared = shares_time_with_next(&stamps(&replica_journal), cut);
    assert_eq!(
        served,
        replayed(&replica_journal),
        "the replica restarted after entry {cut} (its timestamp shared with the next: \
         {shared}) counts otherwise than an uninterrupted replay of its journal"
    );
    (cut, shared)
}

/// A replica promoted after its watermark advanced on the stream it
/// applied goes on from that watermark: its first entries as primary make
/// the tick calls the old primary's next entries would have, and the
/// reports its clients get continue the old primary's count.
///
/// On one host a promoted node's clock is always past its old primary's
/// last timestamp, so a watermark lost on promotion makes no difference
/// a test could see there; it does when the old primary's clock ran
/// ahead of the new one's, as between two hosts. The old primary is
/// scripted to be that host: it streams a history whose every entry is
/// stamped an hour ahead of this host's clock, then dies. The promoted
/// replica's own entries — its promotion, the operator's policy change,
/// a load of client writes — are all stamped behind that time, so an
/// uninterrupted copy makes no tick call for any of them, and every
/// report must carry the count the streamed history left: one, for its
/// first entry. A promotion that restarted the watermark at zero would
/// tick once more on its first entry.
#[test]
fn a_promoted_replica_continues_the_watermark_it_streamed() {
    melin_test_node::logs::capture_if_requested();
    let fx = Fixture::new(0xB5);
    let primary = ScriptedPrimary::bind();
    let ahead = ahead_of_this_host();
    let mut config = fx.replica_config("replica", primary.addr(), AckPolicy::Disk);
    config.admin_bind = Some(endpoint());
    let admin = config.admin_bind.expect("set above");
    let journal = config.journal.clone();
    let replica = spawn_node::<TickCountingApp>(&addrs(0), config);

    let mut stream = primary.accept(Some(0));
    assert_eq!(stream.resumes_after(), 0, "a fresh replica");
    stream_ahead(&mut stream, 1..=STREAMED, ahead);
    // The old primary dies.
    drop(stream);
    drop(primary);

    promote_and_check_it_continues(&fx, replica, admin, &journal, ahead);
}

/// A replica restarted inside a run of entries that share one timestamp
/// goes on from the watermark its replay reached: the rest of the run
/// fires no tick, as on a copy that never stopped.
///
/// The deterministic twin of
/// [`a_replica_restarted_under_load_ticks_as_an_uninterrupted_copy`],
/// whose restart falls inside such a run only by chance: here the old
/// primary is scripted, as in
/// [`a_promoted_replica_continues_the_watermark_it_streamed`], and every
/// entry it streams carries one time, ahead of this host's clock. The
/// replica is stopped half way through the history and restarted, and
/// the primary streams it the rest from where its journal ends. The
/// primary then dies and the replica is promoted and serves, which reads
/// its count: the streamed history's one tick, which a replica that
/// restarted its watermark at zero would have made twice.
#[test]
fn a_replica_restarted_inside_a_run_of_one_timestamp_goes_on_from_its_watermark() {
    melin_test_node::logs::capture_if_requested();
    let fx = Fixture::new(0xBD);
    let primary = ScriptedPrimary::bind();
    let ahead = ahead_of_this_host();
    let replica_addrs = addrs(0);
    let config = |admin| {
        let mut config = fx.replica_config("replica", primary.addr(), AckPolicy::Disk);
        config.admin_bind = admin;
        config
    };
    let journal = config(None).journal;
    let half = STREAMED / 2;

    let replica = spawn_node::<TickCountingApp>(&replica_addrs, config(None));
    let mut stream = primary.accept(Some(0));
    assert_eq!(stream.resumes_after(), 0, "a fresh replica");
    stream_ahead(&mut stream, 1..=half, ahead);
    replica.stop();
    drop(stream);

    let admin = endpoint();
    let replica = spawn_node::<TickCountingApp>(&replica_addrs, config(Some(admin)));
    let mut stream = primary.accept(Some(0));
    assert_eq!(
        stream.resumes_after(),
        half,
        "the restarted replica resumes where its journal ends"
    );
    stream_ahead(&mut stream, half + 1..=STREAMED, ahead);
    // The old primary dies.
    drop(stream);
    drop(primary);

    promote_and_check_it_continues(&fx, replica, admin, &journal, ahead);
}

/// Entries a scripted primary streams in the tests that script one.
const STREAMED: u64 = 400;

/// A time an hour ahead of this host's clock: what a scripted primary on
/// a host whose clock ran ahead stamps its entries with. Nothing this
/// host stamps during a test reaches it.
fn ahead_of_this_host() -> u64 {
    const AHEAD: Duration = Duration::from_secs(3_600);
    melin_app::unix_epoch_nanos() + AHEAD.as_nanos() as u64
}

/// Stream `Add(1)` at every sequence of `sequences`, all stamped
/// `timestamp_ns`, in batches as a primary's journal would send them, and
/// wait for the replica to have journaled the last.
fn stream_ahead(
    stream: &mut scripted_primary::Stream,
    sequences: std::ops::RangeInclusive<u64>,
    timestamp_ns: u64,
) {
    /// Entries per batch. `u64`, as the sequences.
    const PER_BATCH: u64 = 50;
    let last = *sequences.end();
    let mut first = *sequences.start();
    while first <= last {
        let count = PER_BATCH.min(last - first + 1);
        stream.send(
            first,
            timestamp_ns,
            &vec![TestEvent::Add(1); count as usize],
        );
        first += count;
    }
    stream.wait_acked(last);
}

/// Promote `replica`, whose scripted primary streamed it [`STREAMED`]
/// entries all stamped `ahead` and died, have the operator lift the halt
/// over its admin endpoint `admin`, and serve it a load. Everything the
/// promoted node journals is stamped behind `ahead`, so an uninterrupted
/// copy makes no tick call past the streamed history's one, for its first
/// entry: every report, the query, and the replay of the node's journal at
/// `journal` must all say one.
fn promote_and_check_it_continues(
    fx: &Fixture,
    replica: Node,
    admin: SocketAddr,
    journal: &Path,
    ahead: u64,
) {
    /// Bursts each load connection sends to the promoted node.
    const BURSTS: usize = 20;

    admin_reply_when_up(admin, &fx.client_key, "PROMOTE");
    // Its own disk is its only one: the operator consents to serving on
    // it.
    admin_until_ok(admin, &fx.client_key, "ACK-POLICY disk");
    let reports = Load::start(fx, &replica, Some(BURSTS)).finish_served();
    let served = query(fx, &replica);
    replica.stop();

    let stamps = stamps(journal);
    assert!(
        stamps[STREAMED as usize..]
            .iter()
            .all(|&(_, ts)| ts < ahead),
        "the promoted node's own entries are stamped behind the streamed history"
    );
    for reports in &reports {
        assert_eq!(reports.len(), BURSTS * BURST, "every write is reported");
        assert!(
            reports.iter().all(|&ticks| ticks == 1),
            "the promoted node's reports must continue the streamed history's count of 1: \
             {:?}",
            reports
                .iter()
                .filter(|&&ticks| ticks != 1)
                .take(5)
                .collect::<Vec<_>>()
        );
    }
    assert_eq!(served.ticks, 1);
    assert_eq!(served, replayed(journal));
}

/// A node that boots from a snapshot its shadow took under load goes on
/// from the watermark the snapshot records: its replay of the entries
/// after the snapshot makes the tick calls an uninterrupted copy makes,
/// and so does the shadow it starts, whose next snapshot the next boot
/// restores.
///
/// A standalone node with a short snapshot interval, under pipelined
/// writes until its shadow has snapshotted and the journal has gone well
/// past the snapshot, then stopped with them, so its last snapshot is
/// behind its journal's end; then booted again, from that snapshot and
/// the journal after it, and queried before it takes another write. The query must
/// match the replay of the whole journal. The cycle repeats, so that
/// later boots restore snapshots written by shadows that earlier boots
/// started, until at least one snapshot fell inside a run of entries
/// stamped with one time — where a restore that restarted the watermark
/// at zero ticks on the first entry it replays.
#[test]
fn a_node_restored_from_a_shadow_snapshot_ticks_as_an_uninterrupted_copy() {
    /// Boots at most, until a snapshot cuts a run of shared timestamps.
    const BOOTS: usize = 40;
    /// Entries the load journals past the snapshot before the node is
    /// stopped.
    const PAST_SNAPSHOT: u64 = 1_000;

    melin_test_node::logs::capture_if_requested();
    let fx = Fixture::new(0xB9);
    let node_addrs = addrs(0);
    let health = endpoint();
    let config = || {
        let mut config = fx.node_config("node", &fx.primary_key, AckPolicy::Disk);
        config.standalone = true;
        config.health_bind = Some(health);
        // Short, so the shadow snapshots while the load runs.
        config.snapshot_interval_ms = 20;
        config
    };
    let store = config().store().expect("the node's store");
    let (journal, snapshot_path) = (store.journal(), store.snapshot());

    // The sequence each restored snapshot was taken at, and whether it
    // fell inside a run of shared timestamps.
    let mut restored: Vec<(u64, bool)> = Vec::new();
    for boot in 0..=BOOTS {
        let expected = (boot > 0).then(|| {
            let (_, header) =
                melin_transport_core::snapshot::load::<TickCountingApp>(snapshot_path)
                    .expect("the shadow's snapshot");
            restored.push((
                header.sequence,
                shares_time_with_next(&stamps(journal), header.sequence),
            ));
            replayed(journal)
        });
        // The last boot's health endpoint may hold the port a moment past
        // its stop.
        wait_released(&[health]);
        let node = spawn_node::<TickCountingApp>(&node_addrs, config());
        let at_boot = query(&fx, &node);
        if let Some(expected) = expected {
            assert_eq!(
                at_boot,
                expected,
                "booted from the snapshot at {:?}, the node's count differs from an \
                 uninterrupted replay of its journal",
                restored.last()
            );
        }
        if restored.iter().any(|&(_, shared)| shared) {
            node.stop();
            return;
        }

        let load = Load::start(&fx, &node, None);
        let snapshotted = wait_for_snapshot_where::<TickCountingApp>(
            snapshot_path,
            &format!("went past sequence {}", at_boot.sequence),
            |header| header.sequence > at_boot.sequence,
        )
        .sequence;
        wait_for_gauge_where(
            health,
            "melin_journal_sequence",
            "went past the snapshot",
            |v| v >= snapshotted + PAST_SNAPSHOT,
        );
        // Stopped as soon as the writers are: a node on DPDK that stops
        // under them leaves their kernel sockets waiting out their read
        // timeout, with nothing to tell them it has gone.
        load.finish_served();
        node.stop();
    }
    panic!(
        "none of the {} restored snapshots ({restored:?}) fell inside a run of shared \
         timestamps, so none could show an extra tick",
        restored.len()
    );
}
