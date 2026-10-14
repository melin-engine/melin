//! The DPDK replication poll thread, `repl-poll`: a primary's replication
//! on a queue or a NIC of its own, beside the client poll thread rather
//! than inside it.
//!
//! In queue mode ([`ReplicationNic::Queue`]) it polls the client NIC's
//! replication queue, to which the NIC steers replication; in port mode
//! ([`ReplicationNic::Port`]) the replication NIC's one queue. In shared
//! mode it does not run: the client poll thread drives replication on its
//! own transport, as it always has.
//!
//! The thread builds its own transport, on the thread that polls it (a
//! transport never crosses threads), listens on the replication port, and
//! drives the [`DpdkReplicationDriver`] the primary built for it: poll,
//! accept, tick, until shutdown, then closes every replica's link. It is
//! the replication ring's one consumer. Acks reach the response stage's
//! durability gate through the driver's cursors, atomics, as they do from
//! the client poll thread in shared mode; the application's clock ticks
//! stay on the client poll thread, which keeps the input ring single
//! producer.
//!
//! It is pinned and busy-polls as the client poll thread does, and
//! registers with EAL as an lcore of its own so that its mbufs go through
//! a mempool cache rather than the pool's shared ring.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use melin_app::Application;
use melin_dpdk::{
    DpdkConfig, DpdkShared, DpdkTransport, NicConfig, NodeNics, QueueCounters, ReplicationNic,
};
use tracing::{info, warn};

use crate::replication::DpdkReplicationDriver;
use crate::reporting_thread::ReportingThread;

/// Where the replication poll thread builds its transport: a queue of one
/// of the node's NIC groups, configured as that NIC is.
pub(crate) struct ReplicationNicSeat {
    group: Arc<DpdkShared>,
    nic: NicConfig,
    queue: u16,
    /// Whether to send a gratuitous ARP once the transport is up, as the
    /// client NIC's first queue does at boot: on a NIC of its own, whose
    /// MAC the switch has not seen yet. Not on the client NIC's
    /// replication queue, whose MAC the client queue has announced.
    announce: bool,
}

impl ReplicationNicSeat {
    /// The seat of a node whose NICs `nics` were brought up for `config`:
    /// in queue mode the client NIC's replication queue, in port mode the
    /// replication NIC's one queue; `None` in shared mode, which runs no
    /// replication poll thread.
    ///
    /// # Errors
    /// Port mode with no replication NIC brought up, which
    /// [`NodeNics::init`] never returns: a bug, reported rather than run
    /// as shared mode.
    pub(crate) fn of(nics: &NodeNics, config: &DpdkConfig) -> Result<Option<Self>, String> {
        Ok(match &config.replication {
            ReplicationNic::Shared => None,
            ReplicationNic::Queue(_) => Some(ReplicationNicSeat {
                group: Arc::clone(&nics.client),
                nic: config.nic.clone(),
                queue: melin_dpdk::steering::REPLICATION_QUEUE,
                announce: false,
            }),
            ReplicationNic::Port(nic) => Some(ReplicationNicSeat {
                group: nics
                    .replication
                    .as_ref()
                    .map(Arc::clone)
                    .ok_or("port mode, but no replication NIC was brought up")?,
                nic: nic.clone(),
                queue: 0,
                announce: true,
            }),
        })
    }
}

/// Spawn the replication poll thread on `seat`, pinned to `core` (`0`
/// leaves it unpinned), listening for replicas on `listen_port` and
/// driving `driver`, until `shutdown` is set.
///
/// Returns once the thread's transport is up and listening, with the
/// thread and the packet counts of its queue (for `/metrics`), or with the
/// error that kept the transport from coming up, the thread then joined.
/// A panic on the thread sets `shutdown`: in shared mode a panic in the
/// replication driver ends the client poll thread, and with it the node;
/// on a thread of its own it must not leave the node serving clients with
/// no replication behind them. Dropped without being joined (a boot that
/// fails after spawning it), the thread is stopped and joined, so that no
/// thread is left polling a NIC behind the error. See
/// [`crate::reporting_thread`].
pub(crate) fn spawn<A>(
    seat: ReplicationNicSeat,
    listen_port: u16,
    driver: DpdkReplicationDriver<A>,
    core: usize,
    shutdown: Arc<AtomicBool>,
) -> Result<(ReportingThread, Vec<Arc<QueueCounters>>), String>
where
    A: Application + Send + 'static,
{
    let thread_shutdown = Arc::clone(&shutdown);
    crate::reporting_thread::spawn("repl-poll", core, shutdown, move |ready| {
        // The thread is pinned by now: EAL records its affinity as the
        // lcore's. Held to the end of the thread, so that the
        // transport, dropped before it, frees its mbufs through the
        // lcore's cache.
        let _lcore = match seat.group.register_thread() {
            Ok(lcore) => {
                info!(
                    lcore_id = lcore.lcore_id(),
                    "replication poll thread registered with EAL"
                );
                Some(lcore)
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "replication poll thread not registered with EAL: its mbufs go \
                     through the pool's shared ring, without a per-lcore cache"
                );
                None
            }
        };
        let transport = match bring_up(&seat, listen_port) {
            Ok(transport) => transport,
            Err(e) => return ready.report(Err(e)),
        };
        ready.report(Ok(transport.queue_counters()));
        run(transport, driver, &thread_shutdown);
    })
}

/// The thread's transport: built on `seat`, listening for replicas on
/// `listen_port` with the primary's replication buffers, and announced
/// where its NIC is its own.
fn bring_up(seat: &ReplicationNicSeat, listen_port: u16) -> Result<DpdkTransport, String> {
    let mut transport = DpdkTransport::on_nic_unlistening(&seat.group, &seat.nic, seat.queue)
        .map_err(|e| format!("the replication transport: {e}"))?;
    crate::replication::add_replication_listener(&mut transport, listen_port)?;
    if seat.announce {
        // As the client NIC's first queue does at boot: the switch learns
        // this NIC's MAC before a replica dials it.
        transport.send_gratuitous_arp();
    }
    Ok(transport)
}

/// The thread's loop: poll, hand each new connection to the driver, tick,
/// until `shutdown`; then close every replica's link.
fn run<A: Application>(
    mut transport: DpdkTransport,
    mut driver: DpdkReplicationDriver<A>,
    shutdown: &AtomicBool,
) {
    info!("replication poll thread started");
    // The loop's work iterations: those that moved a packet either way,
    // which the transport's own counts say (receiving acks and handshakes,
    // sending the stream). A tick that queues a batch moves its packets at
    // the next poll, so that iteration is the one recorded.
    #[cfg(feature = "latency-trace")]
    let queues = transport.queue_counters();
    #[cfg(feature = "latency-trace")]
    let mut moved = melin_dpdk::packets_moved(&queues);
    #[cfg(feature = "latency-trace")]
    let mut iterations = melin_transport_core::trace::WorkIterations::new(
        melin_transport_core::trace::register_stage(
            "dpdk repl poll: outer iteration (work-iterations only)",
        ),
    );

    while !shutdown.load(Ordering::Relaxed) {
        // Flushes what the last tick queued, and takes in acks.
        transport.poll();
        // The transport's one listener is the replication port's.
        for accepted in transport.take_accepted() {
            driver.accept_connection(accepted.peer, accepted.handle, &mut transport);
        }
        driver.tick(&mut transport, shutdown);

        #[cfg(feature = "latency-trace")]
        {
            let now = melin_dpdk::packets_moved(&queues);
            iterations.end(now != moved, shutdown);
            moved = now;
        }
    }

    // Tell the replicas this primary has gone, as the client poll thread
    // does in shared mode.
    driver.close_all_links(&mut transport);
    info!("replication poll thread stopped");
}
