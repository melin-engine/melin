//! Packets one NIC queue has received and sent, counted by the poll thread
//! that owns the queue and read by any other.
//!
//! What a node puts on `/metrics` to show which queue carries what: with
//! replication on a queue or a port of its own, client traffic on the
//! client queue only and replication on its own is the first thing to check
//! on a rig.
//!
//! Counted in software, at the burst, rather than read from the driver's
//! `rte_eth_stats_get`: that call is not guaranteed thread-safe on every
//! PMD, neither against the queue's own poll thread nor against another
//! thread reading the same port (in queue mode two poll threads share each
//! port), its per-queue fields cover only the first
//! `RTE_ETHDEV_QUEUE_STAT_CNTRS` queues, and some PMDs leave them unset. A
//! count kept by the one thread that bursts the queue needs none of that:
//! it is exact on every PMD, and a reader on another thread only loads two
//! atomics. What it counts is what the poll thread took off the queue and
//! put on it, which is what steering decides; packets the NIC dropped
//! before the queue (its `imissed`) are not in it.
//!
//! Plain atomics with no libdpdk dependency, so ungated and tested on every
//! host, like the rest of the crate's plain logic.

use std::sync::atomic::{AtomicU64, Ordering};

/// One queue's packet counts, on one port. Shared (`Arc`) between the poll
/// thread that counts them and whoever reports them.
///
/// Each count has one writer, the queue's poll thread, which only ever
/// adds to it; so an addition is a load and a store rather than a locked
/// read-modify-write, which a reader can never tear (each is one atomic
/// word) and the poll loop pays nothing for. `u64` counts, which cannot
/// wrap in the life of a process at any packet rate a NIC reaches.
#[derive(Debug)]
pub struct QueueCounters {
    port: u16,
    queue: u16,
    rx_packets: AtomicU64,
    tx_packets: AtomicU64,
}

impl QueueCounters {
    /// Zero counts for queue `queue` of port `port`.
    pub fn new(port: u16, queue: u16) -> Self {
        QueueCounters {
            port,
            queue,
            rx_packets: AtomicU64::new(0),
            tx_packets: AtomicU64::new(0),
        }
    }

    /// The DPDK port the queue is on.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The queue's index on its port.
    pub fn queue(&self) -> u16 {
        self.queue
    }

    /// Packets received on the queue so far.
    pub fn rx_packets(&self) -> u64 {
        self.rx_packets.load(Ordering::Relaxed)
    }

    /// Packets sent on the queue so far.
    pub fn tx_packets(&self) -> u64 {
        self.tx_packets.load(Ordering::Relaxed)
    }

    /// Count `packets` received. Only the queue's poll thread may call
    /// this (the transport's device does): a second writer would lose
    /// counts (see the type's documentation).
    pub fn add_rx(&self, packets: u64) {
        add(&self.rx_packets, packets);
    }

    /// Count `packets` sent. Only the queue's poll thread may call this, as
    /// [`Self::add_rx`].
    pub fn add_tx(&self, packets: u64) {
        add(&self.tx_packets, packets);
    }
}

/// Packets received and sent so far on all of `queues`, together: a poll
/// loop compares it across an iteration to tell whether the iteration
/// moved anything. Wrapping, as a sum of counts that are each a `u64`.
pub fn packets_moved(queues: &[std::sync::Arc<QueueCounters>]) -> u64 {
    queues
        .iter()
        .map(|q| q.rx_packets().wrapping_add(q.tx_packets()))
        .fold(0, u64::wrapping_add)
}

/// `count += packets` for a count with a single writer: `Relaxed`, since
/// nothing else is published through it.
fn add(count: &AtomicU64, packets: u64) {
    count.store(
        count.load(Ordering::Relaxed).wrapping_add(packets),
        Ordering::Relaxed,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn a_queue_starts_at_zero_and_keeps_its_labels() {
        let counters = QueueCounters::new(3, 1);
        assert_eq!((counters.port(), counters.queue()), (3, 1));
        assert_eq!((counters.rx_packets(), counters.tx_packets()), (0, 0));
    }

    #[test]
    fn each_direction_counts_on_its_own() {
        let counters = QueueCounters::new(0, 0);
        counters.add_rx(5);
        counters.add_rx(64);
        counters.add_tx(2);
        assert_eq!(counters.rx_packets(), 69);
        assert_eq!(counters.tx_packets(), 2);
    }

    #[test]
    fn packets_moved_sums_both_directions_of_every_queue() {
        let queues = [
            Arc::new(QueueCounters::new(0, 0)),
            Arc::new(QueueCounters::new(0, 1)),
        ];
        assert_eq!(packets_moved(&queues), 0);
        queues[0].add_rx(4);
        queues[1].add_tx(3);
        assert_eq!(packets_moved(&queues), 7);
        assert_eq!(packets_moved(&[]), 0);
    }

    /// The counting thread's additions are all seen by a thread reading
    /// them, once it has finished: the reporting side of `/metrics`.
    #[test]
    fn another_thread_reads_what_the_poll_thread_counted() {
        let counters = Arc::new(QueueCounters::new(1, 0));
        let writer = Arc::clone(&counters);
        std::thread::spawn(move || {
            for _ in 0..1000 {
                writer.add_rx(3);
                writer.add_tx(1);
            }
        })
        .join()
        .expect("the counting thread");
        assert_eq!(counters.rx_packets(), 3000);
        assert_eq!(counters.tx_packets(), 1000);
    }
}
