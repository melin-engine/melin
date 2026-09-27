//! Failing reproduction for finding 11 of
//! `docs/internal/determinism-audit-2026-09.md`. Gated behind the
//! `determinism-repro` feature; it asserts the documented behaviour and
//! fails until the defect is fixed.

use std::panic::AssertUnwindSafe;

use melin_pipeline::wait::WaitStrategy;

use crate::replication::{REPLICATION_RING_CAPACITY, build_replication_ring};

/// The DPDK sender (server-runtime replication/dpdk.rs) reads a batch,
/// finds it does not fit the TX queue, and breaks without `commit()`,
/// expecting the next tick to read the same batch again. The consumer
/// docs call `try_read` a peek, but it has already advanced.
#[test]
fn an_uncommitted_read_is_delivered_again() {
    let (mut producer, mut consumers) =
        build_replication_ring(1, REPLICATION_RING_CAPACITY, WaitStrategy::SpinThenYield);
    let consumer = &mut consumers[0];
    producer.publish(b"batch ending at 1", 1);
    producer.publish(b"batch ending at 2", 2);

    let first = consumer.try_read().map(|(meta, _)| meta.end_sequence);
    assert_eq!(first, Some(1));
    // No commit: the batch did not fit and will be retried next tick.
    let retried = std::panic::catch_unwind(AssertUnwindSafe(|| {
        consumer.try_read().map(|(meta, _)| meta.end_sequence)
    }));
    match retried {
        Ok(next) => assert_eq!(
            next, first,
            "the retry skipped the uncommitted batch; a commit now releases both, and the \
             replica never receives batch 1"
        ),
        Err(_) => panic!(
            "the retry panicked on the debug assertion; in production this is the DPDK poll \
             thread, which also serves client ingress"
        ),
    }
}
