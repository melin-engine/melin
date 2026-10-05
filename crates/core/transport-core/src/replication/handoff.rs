//! The catch-up→live handoff as a resumable state machine, for a sender
//! that must never wait on the replica or the disk.
//!
//! [`bridge_catchup_to_live`](super::catchup::bridge_catchup_to_live) runs
//! the handoff to completion: it activates the slot's ring, re-reads the
//! journal past the bulk catch-up (the residual pass), then drains the ring
//! into sequence-contiguity, back-filling from disk while a chunk is ahead.
//! The kernel-TCP sender calls it on the replica's own thread, where waiting
//! on the socket or the disk costs nobody else.
//!
//! The DPDK sender runs on the thread that is also client ingress and the
//! only thing running the TCP stack, so it cannot wait for room in a
//! joining replica's socket: a joiner that stopped reading would hold every
//! client on that thread. [`LiveHandoff`] is the same handoff, taken a
//! bounded step at a time ([`LiveHandoff::step`]): each step does what it
//! can without waiting and returns, and the caller steps again on a later
//! poll-loop tick. The decisions are the inline drain's, made by the same
//! classification (`RingChunk::classify`); what changes is only where it
//! waits:
//!
//! - **The journal passes** (residual and back-fill) are started and
//!   advanced through [`HandoffIo`], which on DPDK runs them on the slot's
//!   join worker, off the poll thread, and moves their frames into the
//!   socket a tick's budget at a time.
//! - **A ring chunk the socket has no room for** stays held: the consumer's
//!   read is not committed, so the producer cannot reuse its slot, and the
//!   next step re-borrows it ([`ReplicationConsumer::pending`]) and offers
//!   it again. Nothing is read past it, so nothing is reordered, and it is
//!   committed only once forwarded (or found covered), so nothing is lost.
//!
//! The ring's single-consumer invariant holds as before: only the caller,
//! through `step`, touches the consumer, and the journal passes never do.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use melin_journal::replication::ReplicationConsumer;

use super::catchup::{ChunkFate, HANDOFF_BRIDGE_TIMEOUT, RingChunk};
use super::sent::SentHighWater;

/// What a [`LiveHandoff`] needs from the sender: the replica's socket, and
/// journal passes that run somewhere they may wait.
pub trait HandoffIo {
    /// Start a journal pass: every entry on disk past `from`, framed as the
    /// catch-up frames them, to the replica in order. Exactly what one
    /// [`catch_up_from_journal_with`](super::catchup::catch_up_from_journal_with)
    /// call from `from` streams. At most one pass runs at a time; the
    /// handoff starts the next only once [`Self::pump_pass`] has reported
    /// the last one done.
    fn start_pass(&mut self, from: u64) -> io::Result<()>;

    /// Move the running pass's frames to the replica's socket, as far as
    /// it has room and the caller's budget allows, without waiting on the
    /// socket or the disk.
    fn pump_pass(&mut self) -> PassProgress;

    /// Offer one ring frame to the replica's socket, which takes the whole
    /// frame or none of it. `false` when it has no room now.
    fn try_send(&mut self, frame: &[u8]) -> bool;
}

/// Where the running journal pass stands, from [`HandoffIo::pump_pass`].
#[derive(Debug)]
pub enum PassProgress {
    /// Frames remain: on the disk, or refused by the socket for now.
    Pending,
    /// Every frame of the pass is with the socket: the last sequence it
    /// streamed (`from` when there was nothing new), or why it failed.
    Done(io::Result<u64>),
}

/// What one [`LiveHandoff::step`] came to.
#[derive(Debug)]
pub enum HandoffStep {
    /// Not finished: step again later.
    Pending,
    /// The replica is live: everything up to the ring's next chunk is with
    /// its socket. The slot's sent high-water, to stream live from.
    Live(SentHighWater),
}

/// The catch-up→live handoff in progress. See the module docs.
///
/// Engage the slot's cursors before the first step: the first step stores
/// the active flag (the seed-before-active contract, B2 in
/// `ReplicaCursors`), as `bridge_catchup_to_live` does.
pub struct LiveHandoff {
    /// The replica's handshake position, the floor of the sent high-water.
    handshake_last_sequence: u64,
    phase: Phase,
    /// Seeded once the residual pass is done; meaningless before.
    sent: SentHighWater,
    /// The bound on the drain's wait for the disk: armed when the drain
    /// starts, as the inline drain's, and consulted only when a back-fill
    /// pass has left a chunk still ahead.
    deadline: Option<Instant>,
}

/// Where the handoff is. Each variant names what the next step waits on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Nothing done yet: activate the ring, start the residual pass from
    /// the bulk catch-up's end.
    Start { bulk_catchup_end: u64 },
    /// The residual pass is running.
    Residual,
    /// Draining the ring: the next step reads its next chunk.
    Drain,
    /// A back-fill pass is running for the held chunk (the consumer's
    /// uncommitted read), which was ahead.
    Backfill { chunk: RingChunk },
    /// The held chunk, classified `chunk` when it was found at or ahead of
    /// the position, is due to be forwarded, and the socket refused it.
    /// Offered again as it stands: not re-classified, since a back-fill
    /// may since have passed it, and the inline drain forwards it anyway
    /// (a control frame) or goes live after it (a batch).
    Forward { chunk: RingChunk },
    /// Done; a step here is a caller bug, reported as an error.
    Live,
}

impl LiveHandoff {
    /// A handoff for a replica that handshook at
    /// `handshake_last_sequence` (0 for a divergent one) and whose bulk
    /// catch-up streamed up to `bulk_catchup_end`.
    pub fn new(handshake_last_sequence: u64, bulk_catchup_end: u64) -> Self {
        LiveHandoff {
            handshake_last_sequence,
            phase: Phase::Start { bulk_catchup_end },
            sent: SentHighWater::seed(handshake_last_sequence, bulk_catchup_end),
            deadline: None,
        }
    }

    /// Advance the handoff as far as it goes without waiting, and return.
    ///
    /// Bounded: what the journal passes move is bounded by the caller's
    /// [`HandoffIo`], and the ring gives up at most one `InputBatch` chunk,
    /// plus the covered chunks and control frames ahead of it (no more
    /// than the ring holds). A pass still running when the step ends is
    /// resumed by the next. `now` is the clock the disk-wait bound is read
    /// from.
    ///
    /// `Err` ends the handoff: the caller drops the replica, which
    /// reconnects. The consumer may then hold an uncommitted read; the
    /// caller releases it (commit, or skip to the producer) before the
    /// ring is drained again.
    pub fn step(
        &mut self,
        consumer: &mut ReplicationConsumer,
        active_flag: &AtomicBool,
        io: &mut dyn HandoffIo,
        now: Instant,
    ) -> io::Result<HandoffStep> {
        loop {
            match self.phase {
                Phase::Start { bulk_catchup_end } => {
                    // Activate first: from this store on, a batch whose
                    // publish-check sees the flag is published to the ring
                    // (or evicts the replica); the residual pass re-reads
                    // the ones that fell before it.
                    active_flag.store(true, Ordering::Release);
                    io.start_pass(bulk_catchup_end)?;
                    self.phase = Phase::Residual;
                }
                Phase::Residual => match io.pump_pass() {
                    PassProgress::Pending => return Ok(HandoffStep::Pending),
                    PassProgress::Done(end) => {
                        self.sent = SentHighWater::seed(self.handshake_last_sequence, end?);
                        self.deadline = Some(now + HANDOFF_BRIDGE_TIMEOUT);
                        self.phase = Phase::Drain;
                    }
                },
                Phase::Drain => {
                    let Some((meta, data)) = consumer.try_read() else {
                        // Nothing past the handoff point: go live (see
                        // `drain_into_contiguity` for why an empty ring is
                        // not waited on).
                        return Ok(self.go_live());
                    };
                    let chunk = RingChunk::classify(meta.end_sequence, data, self.sent.get())?;
                    match chunk.fate {
                        ChunkFate::Covered => consumer.commit(),
                        ChunkFate::Ahead => {
                            // Hold the read (the passes never touch the
                            // consumer) and back-fill from disk.
                            io.start_pass(self.sent.get())?;
                            self.phase = Phase::Backfill { chunk };
                        }
                        ChunkFate::Next => {
                            if let Some(step) = self.forward(chunk, consumer, io)? {
                                return Ok(step);
                            }
                        }
                    }
                }
                Phase::Backfill { chunk } => match io.pump_pass() {
                    PassProgress::Pending => return Ok(HandoffStep::Pending),
                    PassProgress::Done(end) => {
                        self.sent.advance(end?);
                        let (meta, data) = held(consumer)?;
                        let fate =
                            RingChunk::classify(meta.end_sequence, data, self.sent.get())?.fate;
                        if fate == ChunkFate::Ahead {
                            if self.deadline.is_some_and(|d| now >= d) {
                                return Err(chunk.stalled(meta.end_sequence, self.sent.get()));
                            }
                            // Not durable yet: read the disk again (the
                            // inline drain's retry). Pumped from the next
                            // step on.
                            io.start_pass(self.sent.get())?;
                            return Ok(HandoffStep::Pending);
                        }
                        // Reached: forward it as the inline drain does.
                        if let Some(step) = self.forward(chunk, consumer, io)? {
                            return Ok(step);
                        }
                    }
                },
                Phase::Forward { chunk } => {
                    if let Some(step) = self.forward(chunk, consumer, io)? {
                        return Ok(step);
                    }
                }
                Phase::Live => {
                    return Err(io::Error::other(
                        "catch-up handoff stepped after going live",
                    ));
                }
            }
        }
    }

    /// Forward the held chunk, classified `chunk` when it was found at or
    /// ahead of the position: a control frame always, an `InputBatch`
    /// unless a back-fill has since covered it — the inline drain's rule.
    /// Returns the step to end on, or `None` to go on draining.
    ///
    /// A chunk the socket refuses stays held (`Phase::Forward`), so the
    /// next step offers the same bytes again.
    fn forward(
        &mut self,
        chunk: RingChunk,
        consumer: &mut ReplicationConsumer,
        io: &mut dyn HandoffIo,
    ) -> io::Result<Option<HandoffStep>> {
        let (meta, data) = held(consumer)?;
        let send = !chunk.batch || meta.end_sequence > self.sent.get();
        if send && !io.try_send(data) {
            self.phase = Phase::Forward { chunk };
            return Ok(Some(HandoffStep::Pending));
        }
        if send && chunk.batch {
            self.sent.advance(meta.end_sequence);
        }
        consumer.commit();
        if chunk.batch {
            return Ok(Some(self.go_live()));
        }
        self.phase = Phase::Drain;
        Ok(None)
    }

    /// End the handoff: the sent high-water is the caller's from here.
    fn go_live(&mut self) -> HandoffStep {
        self.phase = Phase::Live;
        HandoffStep::Live(std::mem::replace(&mut self.sent, SentHighWater::seed(0, 0)))
    }
}

/// The consumer's held read: the chunk a back-fill ran for, or one the
/// socket refused. Always there in the phases that call this, since only
/// `forward` and the covered-chunk arm commit; `Err` rather than a panic
/// all the same, as a bug here must cost the replica its link, not the
/// node its poll thread.
fn held(
    consumer: &ReplicationConsumer,
) -> io::Result<(melin_journal::replication::ReplicationMeta, &[u8])> {
    consumer
        .pending()
        .ok_or_else(|| io::Error::other("catch-up handoff: the held ring chunk is gone"))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::time::Duration;

    use melin_journal::replication::{ReplicationProducer, build_replication_ring};
    use melin_journal::{BufferedWriter, JournalEvent, JournalWrite};
    use melin_pipeline::wait::WaitStrategy;

    use super::*;
    use crate::pipeline::InputSlot;
    use crate::replication::catchup::{CatchUpResult, catch_up_from_journal_with};
    use crate::replication::protocol::{PrimaryMessage, decode_primary_message, encode_rotate};
    use crate::replication_wire::{
        MSG_INPUT_BATCH, encode_input_batch, peek_frame_tag, try_decode_input_batch,
    };
    use crate::test_support::TestEvent;

    // -- Fixtures ----------------------------------------------------------

    fn slot(seq: u64) -> InputSlot<TestEvent> {
        InputSlot {
            connection_id: 0,
            key_hash: 0,
            sequence: seq,
            timestamp_ns: 0,
            event: JournalEvent::App(TestEvent::Add(seq)),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        }
    }

    /// An `InputBatch` frame carrying `seqs`: a ring chunk's shape.
    fn batch(seqs: impl IntoIterator<Item = u64>) -> Vec<u8> {
        let slots: Vec<_> = seqs.into_iter().map(slot).collect();
        let mut buf = Vec::new();
        encode_input_batch(&slots, &mut buf).expect("encode");
        buf
    }

    fn rotate(boundary: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_rotate(boundary, &[0x52; 32], &mut buf);
        buf
    }

    /// Capacity 16: a power of two above what any test here publishes.
    fn ring() -> (ReplicationProducer, ReplicationConsumer) {
        let (producer, mut consumers) = build_replication_ring(1, 16, WaitStrategy::SpinThenYield);
        (producer, consumers.pop().expect("one consumer"))
    }

    fn publish(producer: &mut ReplicationProducer, seqs: std::ops::RangeInclusive<u64>) {
        let end = *seqs.end();
        producer.publish(&batch(seqs), end);
    }

    /// A frame as the replica sees it.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Wire {
        Batch(Vec<u64>),
        Rotate(u64),
    }

    fn decode(frame: &[u8]) -> Wire {
        if peek_frame_tag(frame).expect("tag") == MSG_INPUT_BATCH {
            let slots = try_decode_input_batch::<TestEvent>(&frame[4..]).expect("an InputBatch");
            Wire::Batch(slots.iter().map(|s| s.sequence).collect())
        } else {
            match decode_primary_message(&frame[4..]).expect("a control frame") {
                PrimaryMessage::Rotate { boundary_seq, .. } => Wire::Rotate(boundary_seq),
                other => panic!("unexpected control frame {other:?}"),
            }
        }
    }

    /// Every entry sequence on the wire, in order.
    fn entries(wire: &[Vec<u8>]) -> Vec<u64> {
        wire.iter()
            .flat_map(|f| match decode(f) {
                Wire::Batch(seqs) => seqs,
                Wire::Rotate(_) => Vec::new(),
            })
            .collect()
    }

    /// The disk side of a pass: the frames a pass from `from` streams, and
    /// its end. `Err` fails the pass.
    type Disk<'a> = Box<dyn FnMut(u64) -> io::Result<(Vec<Vec<u8>>, u64)> + 'a>;

    /// The socket's choice: whether to take an offered frame.
    type Socket<'a> = Box<dyn FnMut(&[u8]) -> bool + 'a>;

    /// A sender for the handoff to drive: passes read from `disk` when
    /// started and are pumped a frame at a time; the socket takes what
    /// `accept` lets through. Everything the socket took is on `wire`.
    struct FakeIo<'a> {
        disk: Disk<'a>,
        /// The running pass: frames left, and its end.
        pass: Option<(VecDeque<Vec<u8>>, io::Result<u64>)>,
        /// Where every pass started, in order.
        starts: Vec<u64>,
        accept: Socket<'a>,
        wire: Vec<Vec<u8>>,
        /// Offers the socket refused.
        refusals: usize,
    }

    impl<'a> FakeIo<'a> {
        fn new(disk: Disk<'a>) -> Self {
            FakeIo {
                disk,
                pass: None,
                starts: Vec::new(),
                accept: Box::new(|_| true),
                wire: Vec::new(),
                refusals: 0,
            }
        }

        fn offer(&mut self, frame: &[u8]) -> bool {
            if (self.accept)(frame) {
                self.wire.push(frame.to_vec());
                true
            } else {
                self.refusals += 1;
                false
            }
        }
    }

    impl HandoffIo for FakeIo<'_> {
        fn start_pass(&mut self, from: u64) -> io::Result<()> {
            assert!(self.pass.is_none(), "a pass started while another runs");
            self.starts.push(from);
            let pass = (self.disk)(from).map(|(frames, end)| (frames.into(), Ok(end)));
            self.pass = Some(match pass {
                Ok(pass) => pass,
                Err(e) => (VecDeque::new(), Err(e)),
            });
            Ok(())
        }

        /// One frame per pump at most, so a pass spans several steps.
        fn pump_pass(&mut self) -> PassProgress {
            let (frames, _) = self.pass.as_mut().expect("pumped with no pass running");
            if let Some(frame) = frames.front().cloned() {
                if self.offer(&frame) {
                    self.pass.as_mut().expect("running").0.pop_front();
                }
                return PassProgress::Pending;
            }
            let (_, end) = self.pass.take().expect("running");
            PassProgress::Done(end)
        }

        fn try_send(&mut self, frame: &[u8]) -> bool {
            self.offer(frame)
        }
    }

    /// A pass over a real journal: what `catch_up_from_journal_with`
    /// streams from `from`.
    fn journal_disk(path: &std::path::Path) -> Disk<'_> {
        Box::new(move |from| {
            let never = AtomicBool::new(false);
            let mut frames = Vec::new();
            let mut publish = |f: &[u8]| -> io::Result<()> {
                frames.push(f.to_vec());
                Ok(())
            };
            match catch_up_from_journal_with::<TestEvent>(path, from, &mut publish, &never)? {
                CatchUpResult::Ok(end) => Ok((frames, end)),
                CatchUpResult::NeedSnapshot => Err(io::Error::other("pruned")),
            }
        })
    }

    fn append(writer: &mut BufferedWriter<TestEvent>, seqs: std::ops::RangeInclusive<u64>) {
        for s in seqs {
            writer
                .append(&JournalEvent::App(TestEvent::Add(s)))
                .expect("append");
        }
    }

    /// Step until the handoff goes live, at most `limit` steps.
    fn run(
        handoff: &mut LiveHandoff,
        consumer: &mut ReplicationConsumer,
        active: &AtomicBool,
        io: &mut FakeIo<'_>,
        limit: usize,
    ) -> SentHighWater {
        let now = Instant::now();
        for _ in 0..limit {
            match handoff
                .step(consumer, active, io, now)
                .expect("the handoff fails")
            {
                HandoffStep::Pending => {}
                HandoffStep::Live(sent) => return sent,
            }
        }
        panic!("the handoff did not go live within {limit} steps");
    }

    // -- Tests -------------------------------------------------------------

    /// The inline bridge's regression case, taken a step at a time over a
    /// socket that refuses every other offer: entries journaled after the
    /// bulk catch-up but before activation come off the disk, then the
    /// ring's first chunk — one dense stream, each entry once.
    #[test]
    fn the_window_before_activation_is_replayed_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("j.journal");
        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        append(&mut writer, 1..=12); // 11..=12: after the bulk pass's end.
        append(&mut writer, 13..=14);
        let (mut producer, mut consumer) = ring();
        publish(&mut producer, 13..=14);

        let active = AtomicBool::new(false);
        let mut io = FakeIo::new(journal_disk(&path));
        // Refuses the first offer, takes the second, and so on.
        let mut flip = true;
        io.accept = Box::new(move |_| {
            flip = !flip;
            flip
        });
        let mut handoff = LiveHandoff::new(4, 10);
        let sent = run(&mut handoff, &mut consumer, &active, &mut io, 100);

        assert!(
            active.load(Ordering::Acquire),
            "the handoff activates the ring"
        );
        assert_eq!(io.starts, [10], "one residual pass, from the bulk's end");
        assert_eq!(entries(&io.wire), (11..=14).collect::<Vec<_>>());
        assert!(io.refusals > 0, "the socket refused along the way");
        assert_eq!(sent.get(), 14);
    }

    /// Entries keep arriving while the handoff waits on the socket: those
    /// journaled and published after the residual pass reached the disk
    /// stay in the ring, and the stream switches to the ring exactly where
    /// the disk left off. The first live batch is forwarded; the rest is
    /// the live stream's, in order.
    #[test]
    fn entries_arriving_mid_handoff_join_the_stream_at_the_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("j.journal");
        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        append(&mut writer, 1..=12);
        let (mut producer, mut consumer) = ring();

        let active = AtomicBool::new(false);
        let socket_open = std::cell::Cell::new(false);
        let mut io = FakeIo::new(journal_disk(&path));
        io.accept = Box::new(|_| socket_open.get());
        let mut handoff = LiveHandoff::new(4, 10);
        let now = Instant::now();

        // The socket is full: the residual pass (11..=12) is read but none
        // of it is taken, for many ticks.
        for _ in 0..5 {
            let step = handoff
                .step(&mut consumer, &active, &mut io, now)
                .expect("pending");
            assert!(matches!(step, HandoffStep::Pending));
        }
        assert!(io.wire.is_empty());

        // Meanwhile the journal stage, with the ring active, journals and
        // publishes 13..=16 in two batches.
        assert!(active.load(Ordering::Acquire));
        append(&mut writer, 13..=14);
        publish(&mut producer, 13..=14);
        append(&mut writer, 15..=16);
        publish(&mut producer, 15..=16);

        socket_open.set(true);
        let sent = run(&mut handoff, &mut consumer, &active, &mut io, 100);
        assert_eq!(entries(&io.wire), (11..=14).collect::<Vec<_>>());
        assert_eq!(sent.get(), 14);
        // The live stream goes on from the very next chunk.
        let (meta, data) = consumer.try_read().expect("the second batch is left");
        assert_eq!(decode(data), Wire::Batch(vec![15, 16]));
        assert_eq!(meta.end_sequence, 16);
    }

    /// The ring's first live chunk starts past what the disk has: the
    /// entries between were skipped from the ring and are not durable yet.
    /// The handoff holds the chunk and re-reads the disk on later steps
    /// until they land, then forwards them and the chunk, in order.
    #[test]
    fn a_gap_is_backfilled_over_several_steps() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("j.journal");
        let mut writer = BufferedWriter::<TestEvent>::create(&path).unwrap();
        append(&mut writer, 1..=12);
        let (mut producer, mut consumer) = ring();
        publish(&mut producer, 15..=16); // 13..=14 in flight to the disk.

        let active = AtomicBool::new(false);
        let mut io = FakeIo::new(journal_disk(&path));
        let mut handoff = LiveHandoff::new(4, 10);
        let now = Instant::now();
        for _ in 0..20 {
            assert!(matches!(
                handoff.step(&mut consumer, &active, &mut io, now).unwrap(),
                HandoffStep::Pending
            ));
        }
        assert_eq!(entries(&io.wire), [11, 12], "nothing past the gap yet");
        assert!(
            io.starts.len() > 2,
            "the disk was read again: {:?}",
            io.starts
        );
        assert!(io.starts[1..].iter().all(|&s| s == 12));

        append(&mut writer, 13..=14);
        let sent = run(&mut handoff, &mut consumer, &active, &mut io, 100);
        assert_eq!(entries(&io.wire), (11..=16).collect::<Vec<_>>());
        assert_eq!(sent.get(), 16);
    }

    /// A ring chunk the socket refuses is held, not lost and not read
    /// past: offered again on every step until taken, once, and only
    /// then committed.
    #[test]
    fn a_refused_chunk_is_offered_again_until_taken() {
        let (mut producer, mut consumer) = ring();
        publish(&mut producer, 11..=12);
        publish(&mut producer, 13..=14);

        let active = AtomicBool::new(false);
        let offers = std::cell::Cell::new(0usize);
        let mut io = FakeIo::new(Box::new(|from| Ok((Vec::new(), from))));
        io.accept = Box::new(|frame| {
            assert_eq!(
                decode(frame),
                Wire::Batch(vec![11, 12]),
                "only the held chunk"
            );
            offers.set(offers.get() + 1);
            offers.get() > 3
        });
        let mut handoff = LiveHandoff::new(10, 10);
        let now = Instant::now();
        for _ in 0..3 {
            assert!(matches!(
                handoff.step(&mut consumer, &active, &mut io, now).unwrap(),
                HandoffStep::Pending
            ));
            let (meta, _) = consumer.pending().expect("the chunk is held");
            assert_eq!(meta.end_sequence, 12);
        }
        match handoff.step(&mut consumer, &active, &mut io, now).unwrap() {
            HandoffStep::Live(sent) => assert_eq!(sent.get(), 12),
            HandoffStep::Pending => panic!("the socket took the chunk"),
        }
        assert_eq!(offers.get(), 4);
        assert_eq!(entries(&io.wire), [11, 12], "taken once");
        assert!(consumer.pending().is_none(), "committed once taken");
        let (meta, _) = consumer
            .try_read()
            .expect("the next chunk is the live stream's");
        assert_eq!(meta.end_sequence, 14);
    }

    /// A control frame the back-fill has since passed is still forwarded,
    /// as the inline drain does, even when the socket first refuses it:
    /// the refused offer is not re-judged against the moved position.
    #[test]
    fn a_refused_control_frame_reached_by_a_backfill_is_still_forwarded() {
        let (mut producer, mut consumer) = ring();
        producer.publish(&rotate(12), 12);
        publish(&mut producer, 13..=14);

        let active = AtomicBool::new(false);
        let mut refuse_rotate = true;
        let mut passes = 0;
        let mut io = FakeIo::new(Box::new(move |from| {
            passes += 1;
            // The residual pass finds nothing new; the back-fill then
            // reads past the boundary.
            Ok(if passes > 1 && from < 13 {
                (vec![batch(from + 1..=13)], 13)
            } else {
                (Vec::new(), from)
            })
        }));
        io.accept = Box::new(move |frame| {
            if decode(frame) == Wire::Rotate(12) && refuse_rotate {
                refuse_rotate = false;
                return false;
            }
            true
        });
        let mut handoff = LiveHandoff::new(10, 10);
        let sent = run(&mut handoff, &mut consumer, &active, &mut io, 100);
        let wire: Vec<Wire> = io.wire.iter().map(|f| decode(f)).collect();
        assert_eq!(
            wire,
            [
                Wire::Batch(vec![11, 12, 13]),
                Wire::Rotate(12),
                Wire::Batch(vec![13, 14]),
            ],
            "the rotate is forwarded, and the batch after it (13 again: the receiver drops it)"
        );
        assert_eq!(sent.get(), 14);
    }

    /// A disk that never catches up to the held chunk ends the handoff on
    /// the bridge's bound, measured on the caller's clock, with nothing
    /// past the gap on the wire.
    #[test]
    fn a_stalled_disk_fails_the_handoff_at_the_bound() {
        let (mut producer, mut consumer) = ring();
        publish(&mut producer, 13..=14);

        let active = AtomicBool::new(false);
        let mut io = FakeIo::new(Box::new(|from| Ok((Vec::new(), from))));
        let mut handoff = LiveHandoff::new(10, 10);
        let t0 = Instant::now();
        for ms in [0, 10, 29] {
            assert!(matches!(
                handoff
                    .step(
                        &mut consumer,
                        &active,
                        &mut io,
                        t0 + Duration::from_millis(ms)
                    )
                    .unwrap(),
                HandoffStep::Pending
            ));
        }
        // A pass that is still running is not cut short by the bound.
        let late = t0 + HANDOFF_BRIDGE_TIMEOUT + Duration::from_millis(1);
        let mut result = handoff.step(&mut consumer, &active, &mut io, late);
        if matches!(result, Ok(HandoffStep::Pending)) {
            result = handoff.step(&mut consumer, &active, &mut io, late);
        }
        let err = result.expect_err("the bound has passed");
        assert!(err.to_string().contains("stalled"), "got {err}");
        assert!(io.wire.is_empty(), "nothing past the gap");
    }

    /// A pass that fails (history pruned under it) fails the handoff.
    #[test]
    fn a_failed_pass_fails_the_handoff() {
        let (_producer, mut consumer) = ring();
        let active = AtomicBool::new(false);
        let mut io = FakeIo::new(Box::new(|_| Err(io::Error::other("pruned"))));
        let mut handoff = LiveHandoff::new(10, 10);
        let now = Instant::now();
        let err = loop {
            match handoff.step(&mut consumer, &active, &mut io, now) {
                Ok(HandoffStep::Pending) => {}
                Ok(HandoffStep::Live(_)) => panic!("went live on a failed pass"),
                Err(e) => break e,
            }
        };
        assert!(err.to_string().contains("pruned"));
    }

    /// An empty ring after the residual pass goes live at once: the next
    /// chunk is the live stream's.
    #[test]
    fn an_empty_ring_goes_live_after_the_residual_pass() {
        let (_producer, mut consumer) = ring();
        let active = AtomicBool::new(false);
        let mut io = FakeIo::new(Box::new(|from| {
            Ok((vec![batch(from + 1..=from + 2)], from + 2))
        }));
        let mut handoff = LiveHandoff::new(3, 10);
        let sent = run(&mut handoff, &mut consumer, &active, &mut io, 10);
        assert_eq!(entries(&io.wire), [11, 12]);
        assert_eq!(sent.get(), 12);
        assert!(
            handoff
                .step(&mut consumer, &active, &mut io, Instant::now())
                .is_err(),
            "a finished handoff refuses another step"
        );
    }

    mod parity {
        //! The resumable handoff forwards exactly what the inline one does,
        //! whatever the socket refuses along the way: the same frames, in
        //! the same order, to the same high-water, leaving the ring at the
        //! same chunk.

        use super::*;
        use crate::replication::catchup::drain_into_contiguity;
        use proptest::prelude::*;

        /// A disk whose durable end after the `n`th pass is `ends[n]`
        /// (the last one thereafter); a pass streams one batch of what is
        /// new.
        fn scripted_pass(ends: &[u64], n: usize, from: u64) -> (Vec<Vec<u8>>, u64) {
            let durable = ends[n.min(ends.len() - 1)].max(from);
            let frames = if durable > from {
                vec![batch(from + 1..=durable)]
            } else {
                Vec::new()
            };
            (frames, durable)
        }

        /// What the ring holds: batches from `start`, with a `Rotate` at
        /// some batch boundaries.
        fn fill(producer: &mut ReplicationProducer, start: u64, sizes: &[u64], rotates: &[bool]) {
            let mut next = start;
            for (i, &size) in sizes.iter().enumerate() {
                if rotates[i] {
                    producer.publish(&rotate(next - 1), next - 1);
                }
                let end = next + size - 1;
                publish(producer, next..=end);
                next = end + 1;
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]
            #[test]
            fn matches_the_inline_drain(
                bulk_end in 5u64..20,
                residual_gain in 0u64..6,
                ring_start_offset in 0u64..12,
                sizes in proptest::collection::vec(1u64..4, 0..6),
                rotates in proptest::collection::vec(any::<bool>(), 6),
                extra_ends in proptest::collection::vec(0u64..4, 0..4),
                refusals in proptest::collection::vec(any::<bool>(), 1..8),
            ) {
                // The ring starts anywhere from well behind the residual end
                // to past it; the disk eventually holds every ring entry.
                let residual_end = bulk_end + residual_gain;
                let ring_start = (residual_end + ring_start_offset).saturating_sub(5).max(1);
                let ring_end = ring_start + sizes.iter().sum::<u64>();
                let mut ends = vec![residual_end];
                let mut d = residual_end;
                for gain in &extra_ends {
                    d += gain;
                    ends.push(d);
                }
                ends.push(d.max(ring_end));

                // Inline.
                let (mut producer, mut inline_consumer) = ring();
                fill(&mut producer, ring_start, &sizes, &rotates);
                let mut inline_wire = Vec::new();
                let (residual_frames, end) = scripted_pass(&ends, 0, bulk_end);
                inline_wire.extend(residual_frames);
                let mut inline_sent = SentHighWater::seed(bulk_end, end);
                let mut pass = 1;
                {
                    let wire = std::cell::RefCell::new(&mut inline_wire);
                    let mut forward = |f: &[u8]| -> io::Result<()> {
                        wire.borrow_mut().push(f.to_vec());
                        Ok(())
                    };
                    let mut refill = |from: u64, fwd: &mut dyn FnMut(&[u8]) -> io::Result<()>| {
                        let (frames, end) = scripted_pass(&ends, pass, from);
                        pass += 1;
                        for f in &frames {
                            fwd(f)?;
                        }
                        Ok(end)
                    };
                    drain_into_contiguity(
                        &mut inline_sent,
                        &mut inline_consumer,
                        &mut forward,
                        &mut refill,
                        &mut || false,
                    )
                    .expect("the inline drain succeeds");
                }

                // Resumable, over a socket that refuses per `refusals`.
                let (mut producer, mut consumer) = ring();
                fill(&mut producer, ring_start, &sizes, &rotates);
                let passes = std::cell::Cell::new(0usize);
                let mut io = FakeIo::new(Box::new(|from| {
                    let n = passes.get();
                    passes.set(n + 1);
                    Ok(scripted_pass(&ends, n, from))
                }));
                // At least one offer in each cycle of the pattern is taken,
                // as a socket being drained eventually takes one.
                let mut i = 0;
                io.accept = Box::new(move |_| {
                    i += 1;
                    i % refusals.len() == 0 || !refusals[i % refusals.len()]
                });
                let active = AtomicBool::new(false);
                let mut handoff = LiveHandoff::new(bulk_end, bulk_end);
                let sent = run(&mut handoff, &mut consumer, &active, &mut io, 10_000);

                prop_assert_eq!(&io.wire, &inline_wire);
                prop_assert_eq!(sent.get(), inline_sent.get());
                prop_assert!(consumer.pending().is_none());
                let next = consumer.try_read().map(|(m, _)| m.end_sequence);
                let inline_next = inline_consumer.try_read().map(|(m, _)| m.end_sequence);
                prop_assert_eq!(next, inline_next);
            }
        }
    }
}
