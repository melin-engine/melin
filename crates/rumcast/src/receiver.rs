//! Receiver loop: drains incoming UDP datagrams into the subscription
//! log, detects gaps in the byte stream, sends NAKs (with backoff for
//! multicast NAK suppression), and emits periodic Status Messages so
//! the publisher can drive its flow control.
//!
//! # Threading model
//!
//! The receiver runs on a single network thread, distinct from the
//! subscriber thread that drains the [`SubscriptionLog`] via `poll`.
//! Synchronization with the subscriber is the log's release/acquire
//! protocol; this module performs no additional locking.
//!
//! # NAK suppression
//!
//! On a multicast fan-out, many receivers may see the same gap. To
//! avoid a NAK storm we:
//!
//! 1. Schedule a [`PendingNak`] with a random backoff (50–200 µs)
//!    when a gap is first detected.
//! 2. When the backoff expires, re-check the gap: if a fragment
//!    arrived in the meantime (because another receiver NAKed first
//!    and the publisher's multicast retransmit reached us), suppress
//!    our NAK.
//!
//! For unicast streams (replication) the backoff is wasted but
//! harmless — there's only one receiver.
//!
//! ## Suppression limitation
//!
//! Gap-still-present checks use `partition_high_water_mark` and
//! `subscriber_position`. HWM does NOT decrease when fragments fill
//! the interior of a gap, so suppression only fires once the
//! subscriber thread has actually consumed past the gap via
//! [`SubscriptionLog::poll`]. In deployments where the subscriber
//! polls aggressively (typical), this works as intended. With a slow
//! subscriber, redundant NAKs may go out — wasted bandwidth, not a
//! correctness issue. A proper Aeron-style LossDetector with
//! per-range tracking is a v2 concern.
//!
//! [`SubscriptionLog::poll`]: crate::sub_log::SubscriptionLog::poll

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::counters::{Counters, LossCallback, LossEvent};
use crate::sub_log::{AcceptResult, SubscriptionLog};
use crate::transport::{DatagramBuf, UdpTransport};
use crate::wire::{FrameView, NakFrame, ParseError, StatusMessage, parse_frame, position};

/// Configuration for [`ReceiverLoop`].
#[derive(Debug, Clone, Copy)]
pub struct ReceiverConfig {
    /// Initial publisher address for NAK and Status Message frames.
    /// Acts as a seed only — once the receiver observes the source
    /// addr of a valid Data/Setup/Heartbeat frame from the matching
    /// session/stream, it learns the real publisher endpoint and
    /// retargets all SMs/NAKs there. This avoids cross-wired control
    /// traffic when the publisher binds an ephemeral port (the common
    /// case) and the configured address only happens to land on
    /// *some* socket of the peer rather than the publisher's socket.
    pub dst: SocketAddr,
    /// Unique identifier for this subscriber within the stream. Sent
    /// in every Status Message so the publisher can disambiguate
    /// per-receiver state in a multicast fan-out.
    pub receiver_id: u64,
    /// Send a Status Message every this often.
    pub sm_interval: Duration,
    /// Minimum delay after gap detection before sending a NAK. Sets
    /// a lower bound on suppression-window latency.
    pub nak_backoff_min: Duration,
    /// Maximum random delay added on top of `nak_backoff_min`. Each
    /// receiver picks a uniform random delay in `[min, min+jitter]`,
    /// so peers tend not to NAK simultaneously.
    pub nak_backoff_jitter: Duration,
    /// Maximum recv datagrams to drain per tick. Bounds work per
    /// tick so the loop stays responsive to gap detection / NAK
    /// processing.
    pub max_recv_per_tick: u32,
}

impl ReceiverConfig {
    pub fn defaults(dst: SocketAddr, receiver_id: u64) -> Self {
        Self {
            dst,
            receiver_id,
            sm_interval: Duration::from_millis(100),
            nak_backoff_min: Duration::from_micros(50),
            nak_backoff_jitter: Duration::from_micros(150),
            max_recv_per_tick: 32,
        }
    }
}

/// Per-tick work counters returned by [`ReceiverLoop::tick`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TickStats {
    pub bytes_received: u64,
    pub fragments_accepted: u32,
    pub fragments_dropped: u32,
    pub heartbeats_received: u32,
    pub setups_received: u32,
    pub naks_sent: u32,
    pub naks_suppressed: u32,
    pub sms_sent: u32,
    pub send_errors: u32,
    pub recv_errors: u32,
    pub control_drops: u32,
}

#[derive(Debug, Clone, Copy)]
struct PendingNak {
    term_id: u32,
    term_offset: u32,
    gap_length: u32,
    /// When to re-check the gap and (if still present) send the NAK.
    fire_at: Instant,
}

/// Tiny xorshift64 PRNG, seeded from `receiver_id` so each receiver
/// picks an independent backoff series. Good enough for jitter.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // xorshift's only forbidden state is 0; any non-zero seed
        // produces a full-period 2^64 - 1 cycle.
        Self(seed.max(1))
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    /// Uniform random in `[0, max_exclusive)`. Modulo bias is
    /// negligible at the small ranges we use.
    fn in_range(&mut self, max_exclusive: u64) -> u64 {
        if max_exclusive == 0 {
            0
        } else {
            self.next() % max_exclusive
        }
    }
}

/// Receiver loop. See module docs.
pub struct ReceiverLoop<T: UdpTransport> {
    log: Arc<SubscriptionLog>,
    transport: T,
    config: ReceiverConfig,
    /// Address SMs/NAKs are actually sent to. Seeded from `config.dst`
    /// and overwritten with the source addr of the first valid
    /// Data/Setup/Heartbeat frame from the matching session/stream.
    /// See [`ReceiverConfig::dst`].
    effective_dst: SocketAddr,
    last_sm_at: Instant,
    last_publisher_seen_at: Option<Instant>,
    pending_naks: HashMap<(u32, u32, u32), PendingNak>,
    rng: Rng,
    /// Pool of receive slots reused across ticks for the batched
    /// `recv_batch` call. Sized to `config.max_recv_per_tick`.
    /// Allocated once at construction; the per-tick loop fills them
    /// in place.
    batch_slots: Vec<DatagramBuf>,
    /// Optional cumulative counters folded from each tick's stats.
    counters: Option<Arc<Counters>>,
    /// Optional callback invoked once per detected gap (i.e. when a
    /// NAK is FIRST scheduled — duplicates of an already-pending gap
    /// don't re-fire).
    loss_callback: Option<LossCallback>,
}

impl<T: UdpTransport> ReceiverLoop<T> {
    pub fn new(log: Arc<SubscriptionLog>, transport: T, config: ReceiverConfig) -> Self {
        let now = Instant::now();
        let effective_dst = config.dst;
        let batch_slots = (0..config.max_recv_per_tick)
            .map(|_| DatagramBuf::new(2048))
            .collect();
        Self {
            rng: Rng::new(config.receiver_id),
            log,
            transport,
            config,
            effective_dst,
            last_sm_at: now,
            last_publisher_seen_at: None,
            pending_naks: HashMap::new(),
            batch_slots,
            counters: None,
            loss_callback: None,
        }
    }

    /// Address SMs/NAKs are currently sent to. Equals `config.dst`
    /// until the first valid frame from the publisher arrives, then
    /// the source addr of that frame thereafter.
    pub fn effective_dst(&self) -> SocketAddr {
        self.effective_dst
    }

    /// Install (or remove) shared cumulative counters. Pass `None` to
    /// disable counter updates entirely.
    pub fn set_counters(&mut self, counters: Option<Arc<Counters>>) {
        self.counters = counters;
    }

    /// Install a callback that fires once per detected gap. Pass
    /// `None` to remove. The callback runs on the receiver thread —
    /// keep it cheap.
    ///
    /// Semantics: fires once per *NAK round*, not once per loss
    /// episode. If a gap persists across NAK rounds (publisher
    /// retransmit didn't arrive, gap re-detected on a later tick) the
    /// callback fires again — useful for tracking recovery attempts
    /// rather than just first-detected losses.
    pub fn set_loss_callback(&mut self, cb: Option<LossCallback>) {
        self.loss_callback = cb;
    }

    /// Run one tick. Returns the work performed.
    pub fn tick(&mut self) -> TickStats {
        let mut stats = TickStats::default();
        // One clock read per tick, threaded through every stage.
        // The few-µs drift across stages is far below the smallest
        // interval we check against (NAK backoff_min ~50µs).
        let now = Instant::now();
        self.drain_recv(&mut stats, now);
        self.detect_and_schedule_gap(&mut stats, now);
        self.fire_due_naks(&mut stats, now);
        self.maybe_send_sm(&mut stats, now);
        if let Some(c) = &self.counters {
            fold_into_counters(c, &stats);
        }
        stats
    }

    /// Most-recent instant we received any frame from the publisher
    /// (Data, Setup, or Heartbeat). `None` until first contact.
    pub fn last_publisher_seen_at(&self) -> Option<Instant> {
        self.last_publisher_seen_at
    }

    /// Number of NAKs currently scheduled but not yet fired (waiting
    /// for backoff). Useful for tests and diagnostics.
    pub fn pending_nak_count(&self) -> usize {
        self.pending_naks.len()
    }

    fn drain_recv(&mut self, stats: &mut TickStats, now: Instant) {
        // One batched recv per tick: on KernelUdp this is one
        // `recvmmsg(2)` syscall for up to `max_recv_per_tick` frames;
        // on the io_uring endpoint it's one `Mutex` acquire on the
        // SPSC consumer that drains all available frames in one
        // critical section. Either way: N→1 on the most expensive
        // per-frame cost.
        let n = match self.transport.recv_batch(&mut self.batch_slots) {
            Ok(n) => n,
            Err(_) => {
                stats.recv_errors += 1;
                return;
            }
        };
        if n == 0 {
            return;
        }

        let cfg_session = self.log.config().session_id;
        let cfg_stream = self.log.config().stream_id;

        for slot in &self.batch_slots[..n] {
            let bytes = slot.payload();
            let from = slot.from;
            stats.bytes_received += bytes.len() as u64;

            // Multicast hygiene: drop frames not addressed to our
            // session/stream. last_publisher_seen_at / effective_dst
            // updates only for matching frames so a misaddressed
            // frame doesn't look like liveness from our intended
            // publisher.
            match parse_frame(bytes) {
                Ok(FrameView::Data { header, .. }) => {
                    if header.session_id != cfg_session || header.stream_id != cfg_stream {
                        stats.control_drops += 1;
                        continue;
                    }
                    self.last_publisher_seen_at = Some(now);
                    self.effective_dst = from;
                    let term_id = header.term_id;
                    let term_offset = header.term_offset;
                    let frame_length = header.common.frame_length as usize;
                    let frame = &bytes[..frame_length];
                    match self.log.on_fragment_parsed(term_id, term_offset, frame) {
                        AcceptResult::Accepted => stats.fragments_accepted += 1,
                        _ => stats.fragments_dropped += 1,
                    }
                }
                Ok(FrameView::Setup(s)) => {
                    if s.session_id != cfg_session || s.stream_id != cfg_stream {
                        stats.control_drops += 1;
                    } else {
                        self.last_publisher_seen_at = Some(now);
                        self.effective_dst = from;
                        self.log
                            .advertise_publisher_position(s.active_term_id, s.term_offset);
                        stats.setups_received += 1;
                    }
                }
                Ok(FrameView::Heartbeat(h)) => {
                    if h.session_id != cfg_session || h.stream_id != cfg_stream {
                        stats.control_drops += 1;
                    } else {
                        self.last_publisher_seen_at = Some(now);
                        self.effective_dst = from;
                        stats.heartbeats_received += 1;
                    }
                }
                // Receiver doesn't process NAK / SM — sender-bound.
                Ok(_) => stats.control_drops += 1,
                Err(ParseError::Misaligned) | Err(_) => stats.control_drops += 1,
            }
        }
    }

    fn detect_and_schedule_gap(&mut self, _stats: &mut TickStats, now: Instant) {
        let bits = self.log.term_length_bits();
        let sub_pos = self.log.subscriber_position();
        let sub_term_id = (sub_pos >> bits) as u32;
        let sub_offset = (sub_pos & ((1u64 << bits) - 1)) as u32;

        // Find the partition holding the subscriber's current term.
        let Some(partition) = (0..3).find(|&p| self.log.partition_term_id(p) == sub_term_id) else {
            return;
        };

        let hwm = self.log.partition_high_water_mark(partition);
        if hwm <= sub_offset {
            return; // no gap, fully contiguous up to HWM
        }

        let gap_length = hwm - sub_offset;
        let key = (sub_term_id, sub_offset, gap_length);
        if self.pending_naks.contains_key(&key) {
            return; // already scheduled
        }

        // First detection of this gap: fire the optional loss
        // callback and bump the counters. Repeat detections of the
        // same (term_id, offset, gap_length) tuple are deduped above
        // so the callback is invoked at most once per distinct gap.
        if let Some(c) = &self.counters {
            c.gaps_detected
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            c.bytes_in_gaps
                .fetch_add(gap_length as u64, std::sync::atomic::Ordering::Relaxed);
        }
        if let Some(cb) = &self.loss_callback {
            cb(&LossEvent {
                session_id: self.log.config().session_id,
                stream_id: self.log.config().stream_id,
                term_id: sub_term_id,
                term_offset: sub_offset,
                gap_length,
                detected_at: now,
            });
        }

        // Pick a random backoff in [min, min + jitter].
        let jitter_us = self
            .rng
            .in_range(self.config.nak_backoff_jitter.as_micros().max(1) as u64);
        let fire_at = now + self.config.nak_backoff_min + Duration::from_micros(jitter_us);
        self.pending_naks.insert(
            key,
            PendingNak {
                term_id: sub_term_id,
                term_offset: sub_offset,
                gap_length,
                fire_at,
            },
        );
    }

    fn fire_due_naks(&mut self, stats: &mut TickStats, now: Instant) {
        let bits = self.log.term_length_bits();
        let sub_pos = self.log.subscriber_position();
        let session_id = self.log.config().session_id;
        let stream_id = self.log.config().stream_id;
        // Captured before the retain closure mutably borrows
        // pending_naks — keeps the closure body free of &self.
        let log = &self.log;
        let transport = &self.transport;
        let effective_dst = self.effective_dst;

        // retain folds the "fire or suppress, then drop" pattern into
        // one pass with no temp Vec — the previous two-pass version
        // allocated a key buffer per call.
        self.pending_naks.retain(|_key, pending| {
            if pending.fire_at > now {
                return true;
            }
            // Re-check the gap. `subscriber_position` may have
            // advanced (gap filled and consumed) or the partition's
            // term_id may have rotated (gap moved into the past).
            // Either way: suppress.
            if !gap_still_present(
                log,
                sub_pos,
                bits,
                pending.term_id,
                pending.term_offset,
                pending.gap_length,
            ) {
                stats.naks_suppressed += 1;
                return false;
            }
            let nak = NakFrame::new(
                session_id,
                stream_id,
                pending.term_id,
                pending.term_offset,
                pending.gap_length,
            );
            match transport.send_to(effective_dst, bytemuck::bytes_of(&nak)) {
                Ok(_) => stats.naks_sent += 1,
                Err(_) => stats.send_errors += 1,
            }
            false
        });
    }

    fn maybe_send_sm(&mut self, stats: &mut TickStats, now: Instant) {
        if now.duration_since(self.last_sm_at) < self.config.sm_interval {
            return;
        }
        self.send_sm(stats, now);
    }

    /// Send a Status Message immediately, regardless of `sm_interval`.
    /// Useful at startup so the publisher learns about us before the
    /// first interval elapses.
    pub fn send_sm_now(&mut self) -> TickStats {
        let mut stats = TickStats::default();
        self.send_sm(&mut stats, Instant::now());
        if let Some(c) = &self.counters {
            fold_into_counters(c, &stats);
        }
        stats
    }

    fn send_sm(&mut self, stats: &mut TickStats, now: Instant) {
        let cfg = self.log.config();
        let bits = self.log.term_length_bits();
        let sub_pos = self.log.subscriber_position();
        let term_id = (sub_pos >> bits) as u32;
        let offset = (sub_pos & ((1u64 << bits) - 1)) as u32;
        let window = self.compute_receiver_window(offset);
        let sm = StatusMessage::new(
            cfg.session_id,
            cfg.stream_id,
            term_id,
            offset,
            window,
            self.config.receiver_id,
        );
        match self
            .transport
            .send_to(self.effective_dst, bytemuck::bytes_of(&sm))
        {
            Ok(_) => {
                stats.sms_sent += 1;
                self.last_sm_at = now;
            }
            Err(_) => stats.send_errors += 1,
        }
    }

    /// How much further past `subscriber_position` we'll buffer:
    /// `3 * term_length - bytes_into_active_term`. Capped to u32::MAX
    /// (term_length is bounded ≤ 1 GiB so 3× fits in u32).
    fn compute_receiver_window(&self, in_term_offset: u32) -> u32 {
        let term_length = self.log.config().term_length;
        // 3 * term_length fits in u32 since term_length <= 1 GiB.
        3u32.saturating_mul(term_length)
            .saturating_sub(in_term_offset)
    }
}

/// Re-check whether a scheduled NAK's gap is still missing. The gap
/// `[term_id, term_offset, gap_length)` is still relevant iff:
///   - subscriber_position has not advanced past it (it would have
///     been consumed otherwise), AND
///   - the partition still holds term `term_id`, AND
///   - HWM in that partition is still beyond the gap start.
///
/// Free function (not a method) so `fire_due_naks` can call it inside
/// a `HashMap::retain` closure without re-borrowing `self`.
fn gap_still_present(
    log: &SubscriptionLog,
    sub_pos: u64,
    bits: u32,
    term_id: u32,
    term_offset: u32,
    gap_length: u32,
) -> bool {
    let gap_start = position(term_id, term_offset, bits);
    if sub_pos >= gap_start + gap_length as u64 {
        return false;
    }
    let Some(partition) = (0..3).find(|&p| log.partition_term_id(p) == term_id) else {
        return false;
    };
    // If the subscriber consumed past the gap's start (but not its
    // end), the trailing portion may still be missing — but a fresh
    // NAK will be scheduled from sub_pos forward on the next tick,
    // so suppress this old one. Strict `>`: `sub_pos == gap_start`
    // means no consumption yet and the gap is still entirely relevant.
    if sub_pos > gap_start {
        return false;
    }
    let hwm = log.partition_high_water_mark(partition);
    hwm > term_offset
}

/// Fold a per-tick [`TickStats`] delta into the cumulative
/// [`Counters`]. `Relaxed` ordering throughout — see counters module
/// docs for the consistency contract. `gaps_detected` and
/// `bytes_in_gaps` are bumped at detection time inside
/// [`ReceiverLoop::detect_and_schedule_gap`], not here.
pub(crate) fn fold_into_counters(c: &Counters, s: &TickStats) {
    use std::sync::atomic::Ordering::Relaxed;
    if s.bytes_received != 0 {
        c.bytes_received.fetch_add(s.bytes_received, Relaxed);
    }
    if s.fragments_accepted != 0 {
        c.fragments_accepted
            .fetch_add(s.fragments_accepted as u64, Relaxed);
    }
    if s.fragments_dropped != 0 {
        c.fragments_dropped
            .fetch_add(s.fragments_dropped as u64, Relaxed);
    }
    if s.setups_received != 0 {
        c.setups_received
            .fetch_add(s.setups_received as u64, Relaxed);
    }
    if s.heartbeats_received != 0 {
        c.heartbeats_received
            .fetch_add(s.heartbeats_received as u64, Relaxed);
    }
    if s.naks_sent != 0 {
        c.naks_sent.fetch_add(s.naks_sent as u64, Relaxed);
    }
    if s.naks_suppressed != 0 {
        c.naks_suppressed
            .fetch_add(s.naks_suppressed as u64, Relaxed);
    }
    if s.sms_sent != 0 {
        c.sms_sent.fetch_add(s.sms_sent as u64, Relaxed);
    }
    if s.send_errors != 0 {
        c.send_errors_receiver
            .fetch_add(s.send_errors as u64, Relaxed);
    }
    if s.recv_errors != 0 {
        c.recv_errors.fetch_add(s.recv_errors as u64, Relaxed);
    }
    if s.control_drops != 0 {
        c.control_drops_receiver
            .fetch_add(s.control_drops as u64, Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sub_log::SubscriptionConfig;
    use crate::transport::KernelUdp;
    use crate::wire::{DataFrame, HeartbeatFrame, SetupFrame, data_flags};
    use std::net::{IpAddr, Ipv4Addr};

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    fn sub_cfg() -> SubscriptionConfig {
        SubscriptionConfig {
            session_id: 7,
            stream_id: 11,
            initial_term_id: 100,
            term_length: 64 * 1024,
        }
    }

    fn build_fragment(term_id: u32, term_offset: u32, flags: u8, payload: &[u8]) -> Vec<u8> {
        let header = DataFrame::new(
            sub_cfg().session_id,
            sub_cfg().stream_id,
            term_id,
            term_offset,
            flags,
            payload.len() as u32,
        );
        let mut buf = Vec::with_capacity(DataFrame::HEADER_LEN + payload.len());
        buf.extend_from_slice(bytemuck::bytes_of(&header));
        buf.extend_from_slice(payload);
        buf
    }

    /// Build a receiver paired with a "publisher" socket on localhost.
    /// Returns (log, receiver, publisher_socket, receiver_addr).
    fn build_receiver(
        sm_interval: Duration,
        nak_min: Duration,
        nak_jitter: Duration,
    ) -> (
        Arc<SubscriptionLog>,
        ReceiverLoop<KernelUdp>,
        KernelUdp,
        SocketAddr,
    ) {
        let log = Arc::new(SubscriptionLog::new(sub_cfg()).unwrap());
        let publisher = KernelUdp::bind(loopback(0)).unwrap();
        let publisher_addr = publisher.local_addr().unwrap();
        let recv_socket = KernelUdp::bind(loopback(0)).unwrap();
        let recv_addr = recv_socket.local_addr().unwrap();
        let mut config = ReceiverConfig::defaults(publisher_addr, 42);
        config.sm_interval = sm_interval;
        config.nak_backoff_min = nak_min;
        config.nak_backoff_jitter = nak_jitter;
        let receiver = ReceiverLoop::new(Arc::clone(&log), recv_socket, config);
        (log, receiver, publisher, recv_addr)
    }

    fn drain_n(socket: &KernelUdp, count: usize) -> Vec<Vec<u8>> {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut out: Vec<Vec<u8>> = Vec::new();
        let mut buf = [0u8; 2048];
        while out.len() < count {
            if Instant::now() > deadline {
                panic!("timeout waiting for {count} datagrams; got {}", out.len());
            }
            match socket.recv_from(&mut buf).unwrap() {
                Some((_, len)) => out.push(buf[..len].to_vec()),
                None => std::thread::sleep(Duration::from_micros(100)),
            }
        }
        out
    }

    #[test]
    fn receives_data_fragment_and_makes_it_pollable() {
        let (log, mut receiver, publisher, recv_addr) = build_receiver(
            Duration::from_secs(3600),
            Duration::from_micros(50),
            Duration::from_micros(150),
        );

        let payload = vec![0x55u8; 64];
        let frag = build_fragment(100, 0, data_flags::UNFRAGMENTED, &payload);
        publisher.send_to(recv_addr, &frag).unwrap();

        // Drain until at least one fragment accepted.
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let stats = receiver.tick();
            if stats.fragments_accepted >= 1 {
                break;
            }
            if Instant::now() > deadline {
                panic!("fragment not accepted within deadline");
            }
        }

        // Subscriber-side poll delivers it.
        let mut delivered = Vec::new();
        log.poll(1024, |view| match view {
            FrameView::Data { payload, .. } => delivered.push(payload.to_vec()),
            _ => panic!("expected Data"),
        });
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0], payload);
    }

    #[test]
    fn heartbeat_updates_last_publisher_seen() {
        let (_log, mut receiver, publisher, recv_addr) = build_receiver(
            Duration::from_secs(3600),
            Duration::from_micros(50),
            Duration::from_micros(150),
        );
        assert!(receiver.last_publisher_seen_at().is_none());

        let hb = HeartbeatFrame::new(sub_cfg().session_id, sub_cfg().stream_id);
        publisher
            .send_to(recv_addr, bytemuck::bytes_of(&hb))
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let stats = receiver.tick();
            if stats.heartbeats_received >= 1 {
                break;
            }
            if Instant::now() > deadline {
                panic!("heartbeat not received");
            }
        }
        assert!(receiver.last_publisher_seen_at().is_some());
    }

    #[test]
    fn setup_frame_acknowledged_without_action() {
        let (_log, mut receiver, publisher, recv_addr) = build_receiver(
            Duration::from_secs(3600),
            Duration::from_micros(50),
            Duration::from_micros(150),
        );
        let setup = SetupFrame::new(
            sub_cfg().session_id,
            sub_cfg().stream_id,
            100,
            100,
            0,
            sub_cfg().term_length,
        );
        publisher
            .send_to(recv_addr, bytemuck::bytes_of(&setup))
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let stats = receiver.tick();
            if stats.setups_received >= 1 {
                break;
            }
            if Instant::now() > deadline {
                panic!("setup not received");
            }
        }
    }

    #[test]
    fn periodic_status_message_is_sent() {
        let (_log, mut receiver, publisher, _recv_addr) = build_receiver(
            Duration::from_micros(100),
            Duration::from_micros(50),
            Duration::from_micros(150),
        );
        std::thread::sleep(Duration::from_millis(2));
        let stats = receiver.tick();
        assert!(stats.sms_sent >= 1);

        // Drain the publisher socket to read the SM.
        let dgrams = drain_n(&publisher, 1);
        match parse_frame(&dgrams[0]).unwrap() {
            FrameView::StatusMessage(sm) => {
                assert_eq!(sm.session_id, sub_cfg().session_id);
                assert_eq!(sm.receiver_id, 42);
                assert_eq!(sm.consumption_term_id, 100);
                assert_eq!(sm.consumption_term_offset, 0);
            }
            other => panic!("expected SM, got {other:?}"),
        }
    }

    #[test]
    fn sm_retargets_to_publisher_source_addr_after_first_data_frame() {
        // Regression: in melin's bench wire-up, the receiver's
        // statically-configured `dst` ends up pointing at a *different*
        // socket on the peer (the response receiver port, not the
        // order publisher port). Without auto-discovery, SMs went to
        // the wrong socket and were dropped, so the publisher never
        // learned the consumption position. Verify that after the
        // first valid data frame arrives, subsequent SMs go to the
        // packet's source addr instead of `config.dst`.
        let log = Arc::new(SubscriptionLog::new(sub_cfg()).unwrap());
        let publisher = KernelUdp::bind(loopback(0)).unwrap();
        let publisher_addr = publisher.local_addr().unwrap();
        // A second, unrelated socket — represents the wrong endpoint
        // the receiver was configured with.
        let wrong_dst_socket = KernelUdp::bind(loopback(0)).unwrap();
        let wrong_dst = wrong_dst_socket.local_addr().unwrap();
        let recv_socket = KernelUdp::bind(loopback(0)).unwrap();
        let recv_addr = recv_socket.local_addr().unwrap();
        let mut config = ReceiverConfig::defaults(wrong_dst, 42);
        // Long SM interval so the constructor's first SM doesn't fire
        // before we feed in a data frame.
        config.sm_interval = Duration::from_millis(50);
        let mut receiver = ReceiverLoop::new(Arc::clone(&log), recv_socket, config);
        assert_eq!(receiver.effective_dst(), wrong_dst);

        // Publisher sends a data frame from its real (ephemeral) addr.
        let frag = build_fragment(100, 0, data_flags::UNFRAGMENTED, &[0xAB; 32]);
        publisher.send_to(recv_addr, &frag).unwrap();

        // Tick until the receiver accepts the fragment AND the SM
        // interval elapses. Drain via poll() each round so the
        // subscriber position advances — otherwise unread fragments
        // look like a gap and the receiver fires a NAK before the SM.
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut accepted = false;
        let mut sm_emitted = false;
        while Instant::now() < deadline && !sm_emitted {
            let stats = receiver.tick();
            if stats.fragments_accepted >= 1 {
                accepted = true;
            }
            log.poll(64 * 1024, |_| {});
            if stats.sms_sent >= 1 {
                sm_emitted = true;
            }
        }
        assert!(accepted, "data frame not accepted within deadline");
        assert!(sm_emitted, "no SM emitted within deadline");

        // SM must arrive at the actual publisher socket — not at the
        // wrong-dst socket the receiver was originally configured
        // with.
        let dgrams = drain_n(&publisher, 1);
        match parse_frame(&dgrams[0]).unwrap() {
            FrameView::StatusMessage(sm) => assert_eq!(sm.receiver_id, 42),
            other => panic!("expected SM at publisher addr, got {other:?}"),
        }
        // And the wrong-dst socket must see nothing — SMs should NOT
        // have been mistargeted.
        let mut buf = [0u8; 2048];
        match wrong_dst_socket.recv_from(&mut buf).unwrap() {
            None => {} // expected
            Some((_, len)) => {
                // Whatever it is, it must not be an SM addressed at us
                // — but we expect nothing at all here.
                panic!("wrong-dst socket received {len} bytes; SM was mistargeted");
            }
        }
        assert_eq!(receiver.effective_dst(), publisher_addr);
    }

    #[test]
    fn send_sm_now_emits_sm_immediately() {
        let (_log, mut receiver, publisher, _recv_addr) = build_receiver(
            Duration::from_secs(3600),
            Duration::from_micros(50),
            Duration::from_micros(150),
        );
        let stats = receiver.send_sm_now();
        assert_eq!(stats.sms_sent, 1);
        let _ = drain_n(&publisher, 1);
    }

    #[test]
    fn gap_detected_schedules_pending_nak() {
        let (_log, mut receiver, publisher, recv_addr) = build_receiver(
            Duration::from_secs(3600),
            Duration::from_millis(100), // long backoff so we can observe pending state
            Duration::from_micros(0),
        );
        // Send fragment #2 only — leaves a gap at offset 0.
        let frag = build_fragment(100, 96, data_flags::UNFRAGMENTED, &[0x77u8; 64]);
        publisher.send_to(recv_addr, &frag).unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            receiver.tick();
            if receiver.pending_nak_count() >= 1 {
                break;
            }
            if Instant::now() > deadline {
                panic!("gap not detected");
            }
        }
    }

    #[test]
    fn nak_fired_after_backoff() {
        let (_log, mut receiver, publisher, recv_addr) = build_receiver(
            Duration::from_secs(3600),
            Duration::from_millis(2),
            Duration::from_micros(0),
        );
        // Receive fragment #2, leaving a gap at offset 0.
        let frag = build_fragment(100, 96, data_flags::UNFRAGMENTED, &[0xC9u8; 64]);
        publisher.send_to(recv_addr, &frag).unwrap();

        // Spin ticks until we see naks_sent.
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut total_naks = 0u32;
        loop {
            let stats = receiver.tick();
            total_naks += stats.naks_sent;
            if total_naks >= 1 {
                break;
            }
            if Instant::now() > deadline {
                panic!("NAK not sent (pending={})", receiver.pending_nak_count());
            }
            std::thread::sleep(Duration::from_micros(500));
        }
        // The publisher socket received the NAK.
        let dgrams = drain_n(&publisher, 1);
        match parse_frame(&dgrams[0]).unwrap() {
            FrameView::Nak(n) => {
                assert_eq!(n.term_id, 100);
                assert_eq!(n.term_offset, 0);
                assert_eq!(n.gap_length, 192); // hwm (96+96=192) - sub_pos_offset (0)
            }
            other => panic!("expected NAK, got {other:?}"),
        }
    }

    #[test]
    fn nak_suppressed_when_gap_filled_before_backoff_expires() {
        let (_log, mut receiver, publisher, recv_addr) = build_receiver(
            Duration::from_secs(3600),
            Duration::from_millis(50), // long-ish backoff so we can fill the gap
            Duration::from_micros(0),
        );
        // 1) Receive fragment #2 — schedules NAK for [0, 192).
        let frag2 = build_fragment(100, 96, data_flags::UNFRAGMENTED, &[2u8; 64]);
        publisher.send_to(recv_addr, &frag2).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            receiver.tick();
            if receiver.pending_nak_count() >= 1 {
                break;
            }
            if Instant::now() > deadline {
                panic!("gap not detected");
            }
        }
        // 2) Fill the gap before the backoff fires.
        let frag1 = build_fragment(100, 0, data_flags::UNFRAGMENTED, &[1u8; 64]);
        publisher.send_to(recv_addr, &frag1).unwrap();

        // 3) Subscriber polls so subscriber_position advances past the gap.
        let _log = Arc::clone(&receiver.log);
        // Drain via tick (accepts the new fragment).
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            receiver.tick();
            // We need the subscriber to actually consume — call poll
            // directly on the log.
            receiver.log.poll(1024, |_| {});
            // Wait for backoff to fire. Once it fires, the NAK should
            // be suppressed because the gap is now consumed.
            if Instant::now() > deadline {
                break;
            }
            std::thread::sleep(Duration::from_micros(500));
            if receiver.pending_nak_count() == 0 {
                break;
            }
        }

        // Confirm: no NAK was sent on the publisher socket.
        let mut buf = [0u8; 2048];
        let leftover = publisher.recv_from(&mut buf).unwrap();
        // Could be the SM (we set sm_interval = 1 hr, so no SM). Could
        // be nothing. We assert "not a NAK".
        if let Some((_, len)) = leftover
            && let FrameView::Nak(_) = parse_frame(&buf[..len]).unwrap()
        {
            panic!("NAK sent despite suppression");
        }
    }

    #[test]
    fn fragment_for_wrong_session_dropped() {
        let (log, mut receiver, publisher, recv_addr) = build_receiver(
            Duration::from_secs(3600),
            Duration::from_micros(50),
            Duration::from_micros(150),
        );
        // Build a DataFrame with the WRONG session_id but correct
        // stream_id — should be dropped.
        let bad_session = sub_cfg().session_id.wrapping_add(1);
        let header = DataFrame::new(
            bad_session,
            sub_cfg().stream_id,
            100,
            0,
            data_flags::UNFRAGMENTED,
            32,
        );
        let mut frag = Vec::with_capacity(64);
        frag.extend_from_slice(bytemuck::bytes_of(&header));
        frag.extend_from_slice(&[0u8; 32]);
        publisher.send_to(recv_addr, &frag).unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut total_drops = 0u32;
        loop {
            let stats = receiver.tick();
            total_drops += stats.control_drops;
            if total_drops >= 1 {
                break;
            }
            if Instant::now() > deadline {
                panic!("frame not dropped (drops={total_drops})");
            }
            std::thread::sleep(Duration::from_micros(100));
        }
        // Subscriber sees nothing.
        let mut count = 0;
        log.poll(1024, |_| count += 1);
        assert_eq!(count, 0, "wrong-session frame must not reach subscriber");
        // last_publisher_seen_at must NOT have been bumped.
        assert!(
            receiver.last_publisher_seen_at().is_none(),
            "wrong-session frame must not signal publisher liveness"
        );
    }

    #[test]
    fn heartbeat_for_wrong_stream_dropped() {
        let (_log, mut receiver, publisher, recv_addr) = build_receiver(
            Duration::from_secs(3600),
            Duration::from_micros(50),
            Duration::from_micros(150),
        );
        let bad_stream = sub_cfg().stream_id.wrapping_add(99);
        let hb = HeartbeatFrame::new(sub_cfg().session_id, bad_stream);
        publisher
            .send_to(recv_addr, bytemuck::bytes_of(&hb))
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut total_drops = 0u32;
        loop {
            let stats = receiver.tick();
            total_drops += stats.control_drops;
            if total_drops >= 1 {
                break;
            }
            if Instant::now() > deadline {
                panic!("frame not dropped");
            }
        }
        assert!(
            receiver.last_publisher_seen_at().is_none(),
            "wrong-stream HB must not signal publisher liveness"
        );
    }

    #[test]
    fn status_message_advertises_three_term_window_when_idle() {
        let (_log, mut receiver, publisher, _recv_addr) = build_receiver(
            Duration::from_micros(100),
            Duration::from_micros(50),
            Duration::from_micros(150),
        );
        std::thread::sleep(Duration::from_millis(2));
        let stats = receiver.tick();
        assert!(stats.sms_sent >= 1);

        let dgrams = drain_n(&publisher, 1);
        match parse_frame(&dgrams[0]).unwrap() {
            FrameView::StatusMessage(sm) => {
                // Subscriber at start of term 100 (in_term offset = 0).
                // Window = 3 * term_length - 0.
                assert_eq!(sm.receiver_window, 3 * sub_cfg().term_length);
            }
            other => panic!("expected SM, got {other:?}"),
        }
    }

    #[test]
    fn counters_track_received_bytes_and_fragments() {
        let (_log, mut receiver, publisher, recv_addr) = build_receiver(
            Duration::from_secs(3600),
            Duration::from_micros(50),
            Duration::from_micros(150),
        );
        let counters = Arc::new(Counters::new());
        receiver.set_counters(Some(Arc::clone(&counters)));

        let frag = build_fragment(100, 0, data_flags::UNFRAGMENTED, &[0xAAu8; 64]);
        publisher.send_to(recv_addr, &frag).unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let stats = receiver.tick();
            if stats.fragments_accepted >= 1 {
                break;
            }
            if Instant::now() > deadline {
                panic!("fragment not accepted");
            }
        }
        let snap = counters.snapshot();
        assert!(snap.bytes_received >= 96);
        assert_eq!(snap.fragments_accepted, 1);
    }

    #[test]
    fn loss_callback_fires_once_per_distinct_gap() {
        use std::sync::Mutex;
        let (_log, mut receiver, publisher, recv_addr) = build_receiver(
            Duration::from_secs(3600),
            Duration::from_secs(3600), // long backoff so we observe pending-state
            Duration::from_micros(0),
        );
        let counters = Arc::new(Counters::new());
        receiver.set_counters(Some(Arc::clone(&counters)));

        let collected: Arc<Mutex<Vec<LossEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let collected_for_cb = Arc::clone(&collected);
        receiver.set_loss_callback(Some(Box::new(move |ev: &LossEvent| {
            collected_for_cb.lock().unwrap().push(*ev);
        })));

        // Send fragment #2 at offset 96, leaving a gap at offset 0.
        let frag = build_fragment(100, 96, data_flags::UNFRAGMENTED, &[0xCCu8; 64]);
        publisher.send_to(recv_addr, &frag).unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            receiver.tick();
            if receiver.pending_nak_count() >= 1 {
                break;
            }
            if Instant::now() > deadline {
                panic!("gap not detected");
            }
        }
        // Tick a few more times — same gap, callback must NOT re-fire.
        for _ in 0..5 {
            receiver.tick();
        }
        let events = collected.lock().unwrap().clone();
        assert_eq!(
            events.len(),
            1,
            "callback fired multiple times for same gap"
        );
        let ev = events[0];
        assert_eq!(ev.session_id, sub_cfg().session_id);
        assert_eq!(ev.term_id, 100);
        assert_eq!(ev.term_offset, 0);
        assert_eq!(ev.gap_length, 192);

        let snap = counters.snapshot();
        assert_eq!(snap.gaps_detected, 1);
        assert_eq!(snap.bytes_in_gaps, 192);
    }

    #[test]
    fn loss_callback_re_fires_after_nak_round_completes() {
        // After a pending NAK fires, the gap may still be present
        // (publisher's retransmit hasn't arrived). On the next
        // detect tick, a new pending NAK is scheduled and the
        // callback fires again. This verifies the "per NAK round"
        // semantics documented on set_loss_callback.
        use std::sync::Mutex;
        let (_log, mut receiver, publisher, recv_addr) = build_receiver(
            Duration::from_secs(3600),
            Duration::from_micros(500), // short backoff so the NAK fires fast
            Duration::from_micros(0),
        );
        let collected: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
        let collected_for_cb = Arc::clone(&collected);
        receiver.set_loss_callback(Some(Box::new(move |_ev: &LossEvent| {
            *collected_for_cb.lock().unwrap() += 1;
        })));

        // Send fragment #2; gap stays present (no fragment #1 ever
        // arrives). Two NAK rounds should produce two callback fires.
        let frag = build_fragment(100, 96, data_flags::UNFRAGMENTED, &[0x77u8; 64]);
        publisher.send_to(recv_addr, &frag).unwrap();

        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            receiver.tick();
            if *collected.lock().unwrap() >= 2 {
                break;
            }
            if Instant::now() > deadline {
                panic!(
                    "expected callback to re-fire after NAK round (count={})",
                    *collected.lock().unwrap()
                );
            }
            std::thread::sleep(Duration::from_micros(100));
        }
    }

    #[test]
    fn rng_produces_different_sequences_for_different_seeds() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);
        let av: Vec<u64> = (0..16).map(|_| a.next()).collect();
        let bv: Vec<u64> = (0..16).map(|_| b.next()).collect();
        assert_ne!(av, bv);
    }

    #[test]
    fn rng_in_range_stays_within_bound() {
        let mut r = Rng::new(12345);
        for _ in 0..1000 {
            let v = r.in_range(100);
            assert!(v < 100);
        }
        // in_range(0) returns 0.
        assert_eq!(r.in_range(0), 0);
    }
}
