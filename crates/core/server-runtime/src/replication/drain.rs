//! The replica's drain: the output ring's one consumer on a replica, which
//! has no response stage. It keeps the ring from filling, and, with output
//! digests on, encodes and digests every report the replica's copy of the
//! application produces, by the same rules as the primary's response stage
//! ([`OutputDigester`]), so the two copies' window digests can be compared.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use melin_app::encoder::ResponseEncoder;
use melin_pipeline::ring;
use melin_pipeline::wait::WaitStrategy;
use melin_transport_core::output_digest::{
    BodyBuf, MAX_RESPONSE_BODY, OutputDigester, ReportBody, encode_app_body,
};
use melin_transport_core::pipeline::OutputSlot;

/// Slots the drain takes per read. `usize` because it sizes a slice.
const DRAIN_BATCH: usize = 256;

/// What the drain digests with: the digester and the node's encoder, the
/// same one a primary's response stage encodes replies with.
pub(crate) struct DrainDigest<R: Copy, Q: Copy> {
    pub(crate) digester: OutputDigester,
    pub(crate) encoder: Arc<dyn ResponseEncoder<Report = R, Query = Q>>,
}

/// Run the drain until `shutdown`: read the output ring and, if `digest`
/// is set, digest every report in it.
pub(crate) fn run<R: Copy, Q: Copy>(
    mut consumer: ring::Consumer<OutputSlot<R, Q>>,
    mut digest: Option<DrainDigest<R, Q>>,
    wait: WaitStrategy,
    shutdown: &AtomicBool,
) {
    // Allocated once, before the loop: the slots are copied out of the
    // ring so it can be committed at once, as before digests.
    let mut batch = vec![OutputSlot::<R, Q>::default(); DRAIN_BATCH];
    // One report body at a time, on the stack.
    let mut body: BodyBuf = [0u8; MAX_RESPONSE_BODY];
    let mut waiter = wait.waiter();
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        // Loaded before the read, as the digester requires.
        let applied = digest.as_ref().map(|d| d.digester.load_applied());
        let count = consumer.consume_batch(&mut batch, DRAIN_BATCH);
        if count == 0 {
            if let Some(digest) = digest.as_mut()
                && let Some(applied) = applied
            {
                digest.digester.ring_empty(applied);
            }
            waiter.idle();
            continue;
        }
        waiter.reset();
        if let Some(DrainDigest { digester, encoder }) = digest.as_mut() {
            for slot in &batch[..count] {
                if let Some(at) = digester.slot(slot) {
                    let encoded =
                        encode_app_body(&mut body, |b| encoder.encode_report(at.report(), b));
                    digester.fold(at, ReportBody::of(&body, encoded));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::{Duration, Instant};

    use counter_server::{CounterQuery, CounterReport, ResponseEncoder as CounterEncoder};
    use melin_pipeline::ring::DisruptorBuilder;
    use melin_transport_core::output_digest::{
        BoardLookup, BoardSize, DigestBoardReader, OUTPUT_DIGEST_WINDOW, digest_board,
    };
    use melin_transport_core::pipeline::OutputPayload;
    use melin_transport_core::{AppliedWireSeqCursor, WireSeq};

    use super::*;

    const K: u64 = OUTPUT_DIGEST_WINDOW;

    type Slot = OutputSlot<CounterReport, CounterQuery>;

    /// A running drain digesting from position 1, with the producer that
    /// feeds it, the applied cursor the test publishes, and its board.
    struct Drain {
        producer: ring::Producer<Slot>,
        applied: AppliedWireSeqCursor,
        board: DigestBoardReader,
        shutdown: Arc<AtomicBool>,
        handle: Option<thread::JoinHandle<()>>,
    }

    fn start() -> Drain {
        let (producer, mut consumers) = DisruptorBuilder::<Slot>::new(64)
            .add_consumer()
            .build(WaitStrategy::SpinThenYield);
        let applied = AppliedWireSeqCursor::detached(WireSeq::new(0));
        let (writer, board) = digest_board(BoardSize::new(8).expect("above the minimum"));
        let digester = OutputDigester::new(WireSeq::new(1), applied.clone(), writer);
        let shutdown = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&shutdown);
        let consumer = consumers.pop().expect("one consumer");
        let handle = thread::spawn(move || {
            run(
                consumer,
                Some(DrainDigest {
                    digester,
                    encoder: Arc::new(CounterEncoder),
                }),
                WaitStrategy::SpinThenYield,
                &flag,
            );
        });
        Drain {
            producer,
            applied,
            board,
            shutdown,
            handle: Some(handle),
        }
    }

    impl Drop for Drain {
        fn drop(&mut self) {
            self.shutdown.store(true, Ordering::Relaxed);
            if let Some(handle) = self.handle.take() {
                handle.join().expect("the drain thread");
            }
        }
    }

    fn report(wire_seq: u64, new_value: u64) -> Slot {
        Slot {
            wire_seq,
            payload: OutputPayload::Report(CounterReport::Ack { new_value }),
            ..Default::default()
        }
    }

    /// Wait until the board holds the window ending at `end`.
    fn closed(board: &DigestBoardReader, end: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            match board.get(WireSeq::new(end)) {
                BoardLookup::Closed(_) => return true,
                BoardLookup::Overwritten => panic!("window {end} overwritten"),
                BoardLookup::NotYet => thread::sleep(Duration::from_millis(1)),
            }
        }
        false
    }

    /// A window of reports closes once a slot past it is read.
    #[test]
    fn a_report_past_a_window_closes_it() {
        let mut drain = start();
        drain.producer.publish(report(1, 1));
        drain.producer.publish(report(K + 1, 2));
        assert!(closed(&drain.board, K));
    }

    /// A window of entries that published no slot (a replica's ticks)
    /// closes on the idle path once the applied cursor passes it, and not
    /// before.
    #[test]
    fn a_window_with_no_slot_closes_once_applied_past_it() {
        let drain = start();
        drain.applied.store(WireSeq::new(K - 1));
        thread::sleep(Duration::from_millis(20));
        assert_eq!(drain.board.get(WireSeq::new(K)), BoardLookup::NotYet);
        drain.applied.store(WireSeq::new(K));
        assert!(closed(&drain.board, K));
    }
}
