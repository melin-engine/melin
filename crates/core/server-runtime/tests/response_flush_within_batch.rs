//! The byte-threshold flush must run *within* a batch, not after it.
//!
//! `MAX_SEND_BUF` (64 KiB) drops a connection at append time, so a
//! single `MAX_BATCH` batch whose frames average more than ~64 bytes
//! can push a healthy, actively-reading client from empty past the cap
//! before any batch-end flush runs — torn down with nothing ever
//! written. The 13-byte counter frames can never reach the cap inside
//! one batch (1024 × 13 B ≈ 13 KiB), which is exactly why the sibling
//! `response_flush_before_gate.rs` tests can't see this; this test uses
//! a 400-byte encoder so one batch carries ~160 KiB.
//!
//! Regression shape mirrors `sustained_busy_stream_is_delivered_not_
//! disconnected`: fill the ring behind a shut gate so the slots arrive
//! in as few batches as possible, then open every gate at once.

use std::io::{self, Read, Write};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use melin_app::encoder::ResponseEncoder;
use melin_app::{AppEvent, Application, ApplyCtx, CodecError, RejectReason};
use melin_pipeline::ring::DisruptorBuilder;
use melin_server_runtime::ControlEvent;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::response::{self, Response};
use melin_transport_core::fence::FenceState;
use melin_transport_core::pipeline::{OutputPayload, OutputSlot, StageUtilization};
use melin_transport_core::{DurableWireSeqCursor, WireSeq};
use melin_wire_protocol::blocking::BlockingFrameWriter;

/// Payload bytes per frame (the length prefix counts payload only,
/// matching the runtime's `[len(4) | payload]` wire contract).
const PAYLOAD_LEN: usize = 396;
/// Full frame: prefix + payload. Sized so ~165 slots cross the 64 KiB
/// cap — comfortably inside one 1024-slot batch — while staying under
/// the stage's 512-byte per-frame encode scratch.
const FRAME_LEN: usize = 4 + PAYLOAD_LEN;

/// Enough slots to cross `MAX_SEND_BUF` in one batch several times
/// over (400 × 400 B = 160 KiB), while fitting one ring/batch.
const SLOTS: u64 = 400;

const READ_TIMEOUT: Duration = Duration::from_secs(5);
const SETTLE: Duration = Duration::from_millis(50);

/// Response-stage-only stub: everything the matching stage would call
/// is unreachable, because only `response::run` executes in this test.
struct PadApp;

#[derive(Debug, Clone, Copy)]
struct PadEvent;

impl AppEvent for PadEvent {
    // Never journaled — this test drives the response stage only.
    const MAX_ENCODED_SIZE: usize = 1;

    fn encoded_size(&self) -> usize {
        unreachable!("response-stage-only test")
    }
    fn encode(&self, _buf: &mut [u8]) -> usize {
        unreachable!("response-stage-only test")
    }
    fn decode(_buf: &[u8]) -> Result<Self, CodecError> {
        unreachable!("response-stage-only test")
    }
    fn is_query(&self) -> bool {
        unreachable!("response-stage-only test")
    }
}

/// Carries the slot's sequence so ordering is assertable on the wire.
#[derive(Debug, Clone, Copy)]
struct PadReport {
    value: u64,
}

impl Application for PadApp {
    const APP_VERSION: u16 = 0;

    type Event = PadEvent;
    type Report = PadReport;
    type QueryResponse = PadReport;

    fn apply(
        &mut self,
        _event: Self::Event,
        _ctx: &ApplyCtx,
        _out: &mut Vec<Self::Report>,
    ) -> Option<Self::QueryResponse> {
        unreachable!("response-stage-only test")
    }
    fn tick(&mut self, _now_ns: u64, _out: &mut Vec<Self::Report>) {}
    fn check_request_seq(&mut self, _key_hash: u64, _seq: u64) -> bool {
        unreachable!("response-stage-only test")
    }
    fn build_reject(_event: &Self::Event, _reason: RejectReason) -> Self::Report {
        unreachable!("response-stage-only test")
    }
    fn snapshot<W: Write>(&self, _w: &mut W) -> io::Result<()> {
        unreachable!("response-stage-only test")
    }
    fn restore<R: Read>(_r: &mut R) -> io::Result<Self> {
        unreachable!("response-stage-only test")
    }
}

/// Frame: `[PAYLOAD_LEN(4 LE) | value(8 LE) | zero padding]`.
struct PadEncoder;

impl ResponseEncoder for PadEncoder {
    type Report = PadReport;
    type Query = PadReport;

    fn encode_report(&self, report: &PadReport, buf: &mut [u8]) -> Result<usize, &'static str> {
        buf[..4].copy_from_slice(&(PAYLOAD_LEN as u32).to_le_bytes());
        buf[4..12].copy_from_slice(&report.value.to_le_bytes());
        buf[12..FRAME_LEN].fill(0);
        Ok(FRAME_LEN)
    }

    fn encode_query(&self, query: &Self::Query, buf: &mut [u8]) -> Result<usize, &'static str> {
        self.encode_report(query, buf)
    }
}

fn pad_slot(wire_seq: u64) -> OutputSlot<PadReport, PadReport> {
    OutputSlot {
        connection_id: 1,
        wire_seq,
        payload: OutputPayload::Report(PadReport { value: wire_seq }),
        is_last_in_request: false,
        ..Default::default()
    }
}

fn read_pad(sock: &mut UnixStream) -> io::Result<u64> {
    let mut frame = [0u8; FRAME_LEN];
    sock.read_exact(&mut frame)?;
    assert_eq!(
        u32::from_le_bytes(frame[..4].try_into().expect("4 bytes")),
        PAYLOAD_LEN as u32,
        "unexpected length prefix"
    );
    Ok(u64::from_le_bytes(
        frame[4..12].try_into().expect("8 bytes"),
    ))
}

#[test]
fn large_frame_batch_is_delivered_not_disconnected() {
    let (mut producer, mut consumers) =
        DisruptorBuilder::<OutputSlot<PadReport, PadReport>>::new(1024)
            .add_consumer()
            .build();
    let consumer = consumers.pop().expect("one consumer was requested");

    let (server_sock, mut client_sock) = UnixStream::pair().expect("socketpair");
    let server_fd = server_sock.as_raw_fd();
    let writer = BlockingFrameWriter::new(Box::new(server_sock) as Box<dyn Write + Send>);
    client_sock
        .set_read_timeout(Some(READ_TIMEOUT))
        .expect("set read timeout");

    // Gate shut for everything until the ring is full.
    let journal_cursor = DurableWireSeqCursor::detached(WireSeq::new(0));
    let shutdown = AtomicBool::new(false);
    let (control_tx, control_rx) = mpsc::channel();
    let config = Response::<PadApp> {
        journal_persisted_wire_seq: journal_cursor.clone(),
        ack_policy: Arc::new(AtomicU8::new(AckPolicy::Disk.as_u8())),
        replication_metrics: None,
        replica_active: None,
        heartbeat_interval: None,
        busy_spin: true,
        utilization: Arc::new(StageUtilization::default()),
        encoder: Arc::new(PadEncoder),
        fence_state: Arc::new(FenceState::new(0)),
        active_connections: Arc::new(std::sync::atomic::AtomicU64::new(0)),
    };

    thread::scope(|scope| {
        let stage = scope.spawn(|| {
            response::run::<PadApp>(consumer, control_rx, config, &shutdown);
        });

        control_tx
            .send(ControlEvent::Connected {
                connection_id: 1,
                fd: server_fd,
                writer,
            })
            .expect("stage is running");
        thread::sleep(SETTLE);

        // Fill the ring behind the shut gate: the stage spins on slot
        // 1's gate while the rest accumulate, so they arrive in as few
        // batches as possible — the first post-gate batch alone carries
        // far more than `MAX_SEND_BUF`.
        for i in 1..=SLOTS {
            producer.publish(pad_slot(i));
        }

        // Open every gate at once.
        journal_cursor.store(WireSeq::new(SLOTS));

        // Drain as the stage flushes. Record the outcome and stop the
        // stage BEFORE panicking, or a failure hangs the scope join.
        let mut delivered = 0u64;
        let mut failure: Option<String> = None;
        for i in 1..=SLOTS {
            match read_pad(&mut client_sock) {
                Ok(value) if value == i => delivered += 1,
                Ok(value) => {
                    failure = Some(format!("out of order: got {value}, expected {i}"));
                    break;
                }
                Err(e) => {
                    failure = Some(format!(
                        "response {i} of {SLOTS} never arrived ({e}) — \
                         intra-batch threshold flush missing"
                    ));
                    break;
                }
            }
        }

        shutdown.store(true, Ordering::Relaxed);
        stage.join().expect("response stage panicked");

        if let Some(failure) = failure {
            panic!("{failure}");
        }
        assert_eq!(delivered, SLOTS, "every response delivered");
    });
}
