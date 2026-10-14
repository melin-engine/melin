//! A DPDK node's per-queue packet counts on `/metrics`: what each queue of
//! each port has received and sent, labelled by port and queue, so that a
//! rig shows which queue carries what (client traffic on the client queue,
//! replication on its own queue or port).
//!
//! The counts are kept by the poll thread that owns each queue
//! ([`melin_dpdk::QueueCounters`], which says why they are counted rather
//! than read from the driver) and read here, on the health thread, at each
//! scrape.
//!
//! Ungated, like the counts themselves: the exposition is plain text over
//! atomics, tested on every host; only a DPDK node builds one.

// Without `dpdk` nothing but the tests builds one: the DPDK node is gated.
#![cfg_attr(not(feature = "dpdk"), allow(dead_code))]

use std::io::Write;
use std::sync::Arc;

use melin_dpdk::QueueCounters;
use melin_transport_core::health::TransportMetrics;

/// The node's queues, each with its counts, as one block of `/metrics`.
pub(crate) struct DpdkQueueMetrics {
    /// Sorted by port, then queue, so a scrape lists them in one order
    /// whichever thread brought its queue up first. A `Vec`: a handful of
    /// queues (a client queue and a replication queue, per port of a
    /// bond), built once at boot and walked at each scrape.
    queues: Vec<Arc<QueueCounters>>,
}

impl DpdkQueueMetrics {
    /// The queues `queues` lists, which the node's poll threads count into.
    pub(crate) fn new(mut queues: Vec<Arc<QueueCounters>>) -> Self {
        queues.sort_by_key(|q| (q.port(), q.queue()));
        DpdkQueueMetrics { queues }
    }
}

impl TransportMetrics for DpdkQueueMetrics {
    fn write_prometheus(&self, out: &mut dyn Write) -> std::io::Result<()> {
        // One family per direction, each with its HELP and TYPE once: its
        // name, its help, and the count it reads.
        type Family = (&'static str, &'static str, fn(&QueueCounters) -> u64);
        let families: [Family; 2] = [
            (
                "melin_dpdk_queue_rx_packets_total",
                "Packets the node's poll thread took off each DPDK queue, by port and queue.",
                QueueCounters::rx_packets,
            ),
            (
                "melin_dpdk_queue_tx_packets_total",
                "Packets the node's poll thread put on each DPDK queue, by port and queue; a \
                 bond sends from its first port only.",
                QueueCounters::tx_packets,
            ),
        ];
        for (name, help, count) in families {
            writeln!(out, "# HELP {name} {help}")?;
            writeln!(out, "# TYPE {name} counter")?;
            for queue in &self.queues {
                writeln!(
                    out,
                    "{name}{{port=\"{}\",queue=\"{}\"}} {}",
                    queue.port(),
                    queue.queue(),
                    count(queue)
                )?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exposition(metrics: &DpdkQueueMetrics) -> String {
        let mut out = Vec::new();
        metrics.write_prometheus(&mut out).expect("written");
        String::from_utf8(out).expect("UTF-8")
    }

    /// Each direction is one counter family, its queues labelled by port
    /// and queue in that order, whatever order they came in.
    #[test]
    fn each_queue_is_a_sample_of_each_direction() {
        let replication = Arc::new(QueueCounters::new(0, 1));
        let client = Arc::new(QueueCounters::new(0, 0));
        let other_port = Arc::new(QueueCounters::new(1, 0));
        let metrics = DpdkQueueMetrics::new(vec![
            Arc::clone(&other_port),
            Arc::clone(&replication),
            Arc::clone(&client),
        ]);
        assert_eq!(
            exposition(&metrics),
            "# HELP melin_dpdk_queue_rx_packets_total Packets the node's poll thread took off \
             each DPDK queue, by port and queue.\n\
             # TYPE melin_dpdk_queue_rx_packets_total counter\n\
             melin_dpdk_queue_rx_packets_total{port=\"0\",queue=\"0\"} 0\n\
             melin_dpdk_queue_rx_packets_total{port=\"0\",queue=\"1\"} 0\n\
             melin_dpdk_queue_rx_packets_total{port=\"1\",queue=\"0\"} 0\n\
             # HELP melin_dpdk_queue_tx_packets_total Packets the node's poll thread put on \
             each DPDK queue, by port and queue; a bond sends from its first port only.\n\
             # TYPE melin_dpdk_queue_tx_packets_total counter\n\
             melin_dpdk_queue_tx_packets_total{port=\"0\",queue=\"0\"} 0\n\
             melin_dpdk_queue_tx_packets_total{port=\"0\",queue=\"1\"} 0\n\
             melin_dpdk_queue_tx_packets_total{port=\"1\",queue=\"0\"} 0\n"
        );
    }

    /// A scrape reads the counts as they are then: the metrics hold the
    /// poll threads' counters, not a copy of them.
    #[test]
    fn a_scrape_reads_the_counts_as_they_are() {
        let queues = vec![Arc::new(QueueCounters::new(2, 0))];
        let metrics = DpdkQueueMetrics::new(queues.clone());
        assert!(
            exposition(&metrics)
                .contains("melin_dpdk_queue_rx_packets_total{port=\"2\",queue=\"0\"} 0\n")
        );

        // Counted by the transport's poll thread in a DPDK build; here by
        // the test, the counters' one writer.
        queues[0].add_rx(12);
        queues[0].add_tx(5);
        let text = exposition(&metrics);
        assert!(
            text.contains("melin_dpdk_queue_rx_packets_total{port=\"2\",queue=\"0\"} 12\n"),
            "{text}"
        );
        assert!(
            text.contains("melin_dpdk_queue_tx_packets_total{port=\"2\",queue=\"0\"} 5\n"),
            "{text}"
        );
    }

    /// An exposition cut short by a full buffer, wherever it fills,
    /// reports the error, which the health endpoint takes as truncation,
    /// and one that fits does not.
    #[test]
    fn a_full_buffer_is_an_error_wherever_it_fills() {
        let metrics = DpdkQueueMetrics::new(vec![Arc::new(QueueCounters::new(0, 0))]);
        let whole = exposition(&metrics).len();
        let mut buf = vec![0u8; whole];
        for room in 0..whole {
            let mut cursor = std::io::Cursor::new(&mut buf[..room]);
            assert!(metrics.write_prometheus(&mut cursor).is_err(), "{room}");
        }
        let mut cursor = std::io::Cursor::new(&mut buf[..]);
        assert!(metrics.write_prometheus(&mut cursor).is_ok());
    }

    /// A large node's block, at the widest counts, fits in the room
    /// `/metrics` keeps for it: more queues than either mode has, a client
    /// bond of four ports each with a client and a replication queue (as
    /// in queue mode) and a replication bond of four ports more (as in
    /// port mode). A bond is not bounded in size, so this is the
    /// deployment the budget is sized for, not a limit.
    #[test]
    fn a_large_node_fits_in_the_metrics_budget() {
        let client = (0..4).flat_map(|port| (0..2).map(move |queue| (port, queue)));
        let replication = (4..8).map(|port| (port, 0));
        let queues: Vec<_> = client
            .chain(replication)
            .map(|(port, queue)| {
                let counters = Arc::new(QueueCounters::new(port, queue));
                counters.add_rx(u64::MAX);
                counters.add_tx(u64::MAX);
                counters
            })
            .collect();
        let len = exposition(&DpdkQueueMetrics::new(queues)).len();
        assert!(
            len <= melin_transport_core::health::TRANSPORT_METRICS_BUDGET,
            "{len} bytes"
        );
    }
}
