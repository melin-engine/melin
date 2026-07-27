//! The response stage must not hold an already-durable response in its
//! send buffer while it blocks on a *later* event's durability gate.
//!
//! Steady-state flushing happens on the ring-empty path, which batches
//! many responses behind one `io_uring_enter`. Left alone, that also
//! means a response whose own gate has opened waits out the fsync +
//! replica round-trip of an event sequenced after it — head-of-line
//! blocking between independent requests. The stage therefore drains
//! buffered sends before entering the gate spin.
//!
//! The test drives the stage directly with a hand-held journal cursor so
//! the two events' gates can be opened independently, which is not
//! reproducible through the full server (the journal advances on its own).

use std::io::{Read, Write};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use counter_server::{Counter, CounterQuery, CounterReport, ResponseEncoder};
use melin_pipeline::ring::DisruptorBuilder;
use melin_server_runtime::ControlEvent;
use melin_server_runtime::durability_policy::DurabilityMode;
use melin_server_runtime::response::{self, Response};
use melin_transport_core::fence::FenceState;
use melin_transport_core::pipeline::{OutputPayload, OutputSlot, StageUtilization};
use melin_transport_core::{DurableWireSeqCursor, WireSeq};
use melin_wire_protocol::blocking::BlockingFrameWriter;

/// `CounterReport::Ack` on the wire: len(4) + tag(1) + value(8).
const FRAME_LEN: usize = 13;
/// Payload length the counter encoder writes into the length prefix.
const PAYLOAD_LEN: u32 = 9;

/// Long enough that a genuine hang is distinguishable from scheduler
/// noise on a loaded box, short enough to fail in bounded time.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Settle time between driving an input and asserting on the stage's
/// reaction. The stage busy-spins, so this is generous by orders of
/// magnitude — it exists to order the test's steps, not to wait out work.
const SETTLE: Duration = Duration::from_millis(50);

/// A single gated ack slot. `is_last_in_request: false` suppresses the
/// trailing `BatchEnd` frame so the wire carries exactly one frame per
/// slot and the assertions can read fixed-width.
fn ack_slot(wire_seq: u64, new_value: u64) -> OutputSlot<CounterReport, CounterQuery> {
    OutputSlot {
        connection_id: 1,
        wire_seq,
        payload: OutputPayload::Report(CounterReport::Ack { new_value }),
        is_last_in_request: false,
        ..Default::default()
    }
}

fn read_ack(sock: &mut UnixStream) -> std::io::Result<u64> {
    let mut frame = [0u8; FRAME_LEN];
    sock.read_exact(&mut frame)?;
    assert_eq!(
        u32::from_le_bytes(frame[..4].try_into().expect("4 bytes")),
        PAYLOAD_LEN,
        "unexpected length prefix"
    );
    Ok(u64::from_le_bytes(frame[5..].try_into().expect("8 bytes")))
}

/// Build a response-stage config gating on `journal_cursor` alone.
/// `Local` mode needs no replica wiring, so the cursor is the only input
/// that decides whether the gate is open.
fn config_for(journal_cursor: DurableWireSeqCursor) -> Response<Counter> {
    Response::<Counter> {
        journal_persisted_wire_seq: journal_cursor,
        durability_mode: Arc::new(AtomicU8::new(DurabilityMode::Local.as_u8())),
        replication_metrics: None,
        replica_active: None,
        heartbeat_interval: None,
        busy_spin: true,
        utilization: Arc::new(StageUtilization::default()),
        encoder: Arc::new(ResponseEncoder),
        fence_state: Arc::new(FenceState::new(0)),
    }
}

#[test]
fn durable_response_is_released_before_blocking_on_a_later_gate() {
    let (mut producer, mut consumers) =
        DisruptorBuilder::<OutputSlot<CounterReport, CounterQuery>>::new(1024)
            .add_consumer()
            .build();
    let consumer = consumers.pop().expect("one consumer was requested");

    let (server_sock, mut client_sock) = UnixStream::pair().expect("socketpair");
    let server_fd = server_sock.as_raw_fd();
    let writer = BlockingFrameWriter::new(Box::new(server_sock) as Box<dyn Write + Send>);
    client_sock
        .set_read_timeout(Some(READ_TIMEOUT))
        .expect("set read timeout");

    // Journal cursor starts behind both events, so both gates are shut.
    let journal_cursor = DurableWireSeqCursor::detached(WireSeq::new(0));
    let shutdown = AtomicBool::new(false);
    let (control_tx, control_rx) = mpsc::channel();
    let config = config_for(journal_cursor.clone());

    thread::scope(|scope| {
        let stage = scope.spawn(|| {
            response::run::<Counter>(consumer, control_rx, config, &shutdown);
        });

        control_tx
            .send(ControlEvent::Connected {
                connection_id: 1,
                fd: server_fd,
                writer,
            })
            .expect("stage is running");
        // The stage polls the control channel before consuming output
        // slots, but only once per iteration — publishing before it has
        // registered the connection would drop the slot on an unknown id.
        thread::sleep(SETTLE);

        // Event A enters the ring and the stage blocks on its gate.
        producer.publish(ack_slot(1, 111));
        thread::sleep(SETTLE);

        // Event B arrives while the stage is still spinning on A's gate,
        // guaranteeing the two land in separate `consume_batch` calls —
        // the shape that produces the head-of-line block.
        producer.publish(ack_slot(2, 222));
        thread::sleep(SETTLE);

        // Open A's gate only. The stage appends A, picks up B on the next
        // iteration, and finds B's gate shut. A must go out regardless.
        journal_cursor.store(WireSeq::new(1));
        let first = read_ack(&mut client_sock);

        // B stays gated until its own cursor lands.
        journal_cursor.store(WireSeq::new(2));
        let second = read_ack(&mut client_sock);

        // Stop the stage before asserting. A failed read here means the
        // response never arrived, and panicking with the stage still
        // spinning would leave `thread::scope` waiting on it forever —
        // turning a clean failure into a hung test run.
        shutdown.store(true, Ordering::Relaxed);
        stage.join().expect("response stage panicked");

        let first = first
            .expect("durable response withheld while the stage waited on a later event's gate");
        assert_eq!(first, 111, "wrong response released first");
        let second = second.expect("second response never arrived");
        assert_eq!(second, 222, "responses delivered out of order");
    });
}

/// The gate is per slot, not per batch: a slot whose own `wire_seq` is
/// durable must not wait for a later slot in the *same* batch.
///
/// Batch-max gating took `needed` as the maximum `wire_seq` across the
/// consumed batch, so the oldest response paid the durability latency of
/// the newest event in it — head-of-line blocking bounded only by
/// `MAX_BATCH`.
///
/// Getting two slots into one batch deterministically needs a third
/// event to hold the stage still: while it spins on `hold`'s gate, the
/// two under test accumulate in the ring and are consumed together.
#[test]
fn earlier_slot_in_a_batch_is_not_held_by_a_later_one() {
    let (mut producer, mut consumers) =
        DisruptorBuilder::<OutputSlot<CounterReport, CounterQuery>>::new(1024)
            .add_consumer()
            .build();
    let consumer = consumers.pop().expect("one consumer was requested");

    let (server_sock, mut client_sock) = UnixStream::pair().expect("socketpair");
    let server_fd = server_sock.as_raw_fd();
    let writer = BlockingFrameWriter::new(Box::new(server_sock) as Box<dyn Write + Send>);
    client_sock
        .set_read_timeout(Some(READ_TIMEOUT))
        .expect("set read timeout");

    let journal_cursor = DurableWireSeqCursor::detached(WireSeq::new(0));
    let shutdown = AtomicBool::new(false);
    let (control_tx, control_rx) = mpsc::channel();
    let config = config_for(journal_cursor.clone());

    thread::scope(|scope| {
        let stage = scope.spawn(|| {
            response::run::<Counter>(consumer, control_rx, config, &shutdown);
        });

        control_tx
            .send(ControlEvent::Connected {
                connection_id: 1,
                fd: server_fd,
                writer,
            })
            .expect("stage is running");
        thread::sleep(SETTLE);

        // Hold the stage in a gate wait.
        producer.publish(ack_slot(1, 111));
        thread::sleep(SETTLE);

        // Both accumulate behind the held stage, so they are consumed as
        // one batch whose maximum `wire_seq` is 3.
        producer.publish(ack_slot(2, 222));
        producer.publish(ack_slot(3, 333));
        thread::sleep(SETTLE);

        // Releases the hold *and* event 2, but not event 3. Batch-max
        // gating would make 2 wait for 3; per-slot gating sends it.
        journal_cursor.store(WireSeq::new(2));
        let held = read_ack(&mut client_sock);
        let early = read_ack(&mut client_sock);

        journal_cursor.store(WireSeq::new(3));
        let late = read_ack(&mut client_sock);

        shutdown.store(true, Ordering::Relaxed);
        stage.join().expect("response stage panicked");

        assert_eq!(held.expect("held response never arrived"), 111);
        assert_eq!(
            early.expect("durable slot held behind a later slot in the same batch"),
            222
        );
        assert_eq!(late.expect("last response never arrived"), 333);
    });
}

/// Guards the other half of the trade-off: the pre-gate flush must not
/// change delivery when the gate is already satisfied. With the cursor
/// ahead of every event, the stage takes the fast path through the gate
/// and all responses still arrive, in order, behind the ring-empty flush.
#[test]
fn open_gate_delivers_the_whole_batch_in_order() {
    let (mut producer, mut consumers) =
        DisruptorBuilder::<OutputSlot<CounterReport, CounterQuery>>::new(1024)
            .add_consumer()
            .build();
    let consumer = consumers.pop().expect("one consumer was requested");

    let (server_sock, mut client_sock) = UnixStream::pair().expect("socketpair");
    let server_fd = server_sock.as_raw_fd();
    let writer = BlockingFrameWriter::new(Box::new(server_sock) as Box<dyn Write + Send>);
    client_sock
        .set_read_timeout(Some(READ_TIMEOUT))
        .expect("set read timeout");

    // Cursor already past every event under test — no gate ever waits.
    let journal_cursor = DurableWireSeqCursor::detached(WireSeq::new(100));
    let shutdown = AtomicBool::new(false);
    let (control_tx, control_rx) = mpsc::channel();
    let config = config_for(journal_cursor);

    thread::scope(|scope| {
        let stage = scope.spawn(|| {
            response::run::<Counter>(consumer, control_rx, config, &shutdown);
        });

        control_tx
            .send(ControlEvent::Connected {
                connection_id: 1,
                fd: server_fd,
                writer,
            })
            .expect("stage is running");
        thread::sleep(SETTLE);

        for (seq, value) in [(1u64, 11u64), (2, 22), (3, 33)] {
            producer.publish(ack_slot(seq, value));
        }

        let received: Vec<_> = (0..3).map(|_| read_ack(&mut client_sock)).collect();

        // Same rationale as the sibling test: stop the stage before any
        // assertion can panic, or a failure hangs the run.
        shutdown.store(true, Ordering::Relaxed);
        stage.join().expect("response stage panicked");

        for (value, expected) in received.into_iter().zip([11u64, 22, 33]) {
            let value = value.expect("response never arrived");
            assert_eq!(value, expected, "responses delivered out of order");
        }
    });
}
