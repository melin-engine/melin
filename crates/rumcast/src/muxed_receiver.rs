//! Multi-session subscription receiver.
//!
//! Like [`crate::receiver::ReceiverLoop`] but demultiplexes incoming
//! frames by `header.session_id` into a per-session
//! [`SubscriptionLog`]. Sessions are allocated lazily on first
//! contact with a previously-unseen session_id, capped by
//! [`MuxedReceiverConfig::max_sessions`].
//!
//! This is the receive side of the Phase-3 multi-client wiring:
//! one server, one bound UDP socket, N concurrently-connected
//! clients each with their own `session_id`.
//!
//! # Threading
//!
//! Single-threaded — the embedding code (e.g. `melin-server`'s
//! session translator) drives [`MuxedReceiver::tick`] and
//! [`MuxedReceiver::poll`] on the same thread. Per-session
//! `Arc<SubscriptionLog>` clones may be handed out to other threads
//! for read-only inspection, but the muxer itself owns all
//! per-session bookkeeping (NAK state, last_sm_at, effective_dst).
//!
//! # NAK suppression
//!
//! Same backoff scheme as the single-session receiver, but each
//! session has its own pending-NAK map and PRNG (seeded from
//! `session_id` so two concurrent sessions don't fire NAKs at
//! exactly the same instant).
//!
//! # Known limitation: orphan sessions from transport-only frames
//!
//! Setup and Heartbeat frames at the rumcast wire level allocate a
//! session here just like Data frames do. If a peer sends only
//! Setups/Heartbeats and never any Data, the session sits in the
//! muxer holding `3 * term_length` bytes of buffer until either
//! the embedding server explicitly [`MuxedReceiver::evict`]s it or
//! the process restarts. With `max_sessions = 1024` and
//! `term_length = 1 MiB` this caps the worst case at ~3 GiB, which
//! [`MuxedReceiverConfig::max_sessions`] bounds.
//!
//! The follow-up plan (see Phase 3 task #31) is an idle-GC sweep
//! driven by an `is_data_active` flag — sessions that have only
//! ever received transport-level keepalives get reaped quickly
//! (e.g. seconds), data-active sessions get a longer grace period.
//! Until then, embedders that face a hostile peer should either
//! lower `max_sessions` or implement application-level eviction
//! via `evict()`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::counters::Counters;
use crate::storage::AlignedBuf;
use crate::sub_log::{AcceptResult, SubscriptionConfig, SubscriptionLog};
use crate::transport::UdpTransport;
use crate::wire::{FrameView, NakFrame, ParseError, StatusMessage, parse_frame, position};

// Re-use the per-tick stats type from the single-session receiver
// so observability tooling doesn't need to learn two shapes.
pub use crate::receiver::TickStats;

/// Configuration for [`MuxedReceiver`]. Differs from the single-
/// session [`crate::receiver::ReceiverConfig`] in two ways:
///
/// 1. No `dst` — each session learns its publisher addr from the
///    source addr of incoming frames.
/// 2. Adds `max_sessions`, `initial_term_id`, and `term_length` so
///    the muxer can allocate fresh `SubscriptionLog`s on demand.
#[derive(Debug, Clone, Copy)]
pub struct MuxedReceiverConfig {
    /// Stream ID this receiver subscribes to. Frames with a
    /// different stream_id are dropped as misaddressed. All
    /// per-session sublogs share this single stream_id (each
    /// session has its own session_id but joins the same logical
    /// stream).
    pub stream_id: u32,
    /// Receiver ID stamped into outgoing Status Messages so the
    /// publisher can disambiguate per-receiver flow-control state
    /// in a multicast fan-out. For unicast (our use case) this is
    /// effectively a constant per server instance.
    pub receiver_id: u64,
    /// Initial term_id used when a fresh `SubscriptionLog` is
    /// allocated on first contact for a new session. Must match
    /// the publisher's `initial_term_id`.
    pub initial_term_id: u32,
    /// Per-session term length. Each new session allocates
    /// `3 * term_length` bytes of buffer for its three rotating
    /// partitions, so this knob directly controls the muxer's
    /// memory footprint at scale.
    pub term_length: u32,
    /// Send a Status Message every this often, per session.
    pub sm_interval: Duration,
    /// Minimum delay after gap detection before sending a NAK.
    pub nak_backoff_min: Duration,
    /// Maximum random delay added on top of `nak_backoff_min`.
    pub nak_backoff_jitter: Duration,
    /// Maximum recv datagrams to drain per tick. Bounds work per
    /// tick so the loop stays responsive to gap detection.
    pub max_recv_per_tick: u32,
    /// Maximum concurrent sessions. Frames from a new session_id
    /// arriving past this cap are dropped (and counted in
    /// `sessions_rejected`). Bounds memory at
    /// `max_sessions * 3 * term_length`.
    pub max_sessions: u32,
}

impl MuxedReceiverConfig {
    /// Reasonable defaults — production code should pin
    /// `term_length`, `max_sessions`, and intervals deliberately.
    pub fn defaults(receiver_id: u64) -> Self {
        Self {
            stream_id: 1,
            receiver_id,
            initial_term_id: 1,
            term_length: 1024 * 1024, // 1 MiB — far smaller than the
            // 16 MiB single-session default, since we may have
            // many concurrent sessions and a healthy LAN doesn't
            // need a huge retransmit window.
            sm_interval: Duration::from_millis(100),
            nak_backoff_min: Duration::from_micros(50),
            nak_backoff_jitter: Duration::from_micros(150),
            max_recv_per_tick: 32,
            max_sessions: 1024,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct PendingNak {
    term_id: u32,
    term_offset: u32,
    gap_length: u32,
    fire_at: Instant,
}

/// Tiny xorshift64 PRNG for NAK backoff jitter. Seeded per session
/// so two concurrent sessions don't fire NAKs in lockstep on the
/// same gap.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
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
    fn in_range(&mut self, max_exclusive: u64) -> u64 {
        if max_exclusive == 0 {
            0
        } else {
            self.next() % max_exclusive
        }
    }
}

/// Per-session state owned by the muxer. One of these per
/// distinct `session_id` we've seen.
struct SessionInbound {
    log: Arc<SubscriptionLog>,
    pending_naks: HashMap<(u32, u32, u32), PendingNak>,
    last_sm_at: Instant,
    last_publisher_seen_at: Option<Instant>,
    /// Source addr of the last valid frame from this session's
    /// publisher. SMs and NAKs go here. Initialized from the first
    /// frame's source addr (we never allocate a session without
    /// having one).
    effective_dst: SocketAddr,
    rng: Rng,
}

/// Multi-session subscription receiver. See module docs.
pub struct MuxedReceiver<T: UdpTransport> {
    transport: T,
    config: MuxedReceiverConfig,
    sessions: HashMap<u32, SessionInbound>,
    recv_buf: Box<AlignedBuf<2048>>,
    counters: Option<Arc<Counters>>,
}

impl<T: UdpTransport> MuxedReceiver<T> {
    pub fn new(transport: T, config: MuxedReceiverConfig) -> Self {
        Self {
            transport,
            config,
            sessions: HashMap::new(),
            recv_buf: Box::new(AlignedBuf::new()),
            counters: None,
        }
    }

    pub fn set_counters(&mut self, counters: Option<Arc<Counters>>) {
        self.counters = counters;
    }

    /// Number of currently-allocated sessions. Useful for the
    /// embedding server's health endpoint and for tests.
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Source addr SMs/NAKs are currently sent to for `session_id`,
    /// or `None` if no such session is known. Used by the embedding
    /// server to plumb the per-session dst through to the matching
    /// `MuxedSender::create_session` call when authentication
    /// completes.
    pub fn effective_dst(&self, session_id: u32) -> Option<SocketAddr> {
        self.sessions.get(&session_id).map(|s| s.effective_dst)
    }

    /// Drop a session. Called by the embedding server after auth
    /// failure, handshake timeout, or idle eviction. Idempotent —
    /// removing a session_id that doesn't exist is a no-op.
    pub fn evict(&mut self, session_id: u32) {
        self.sessions.remove(&session_id);
    }

    /// Iterate over `(session_id, &Arc<SubscriptionLog>)` pairs.
    /// Mostly useful for the session translator to drain each
    /// log in turn.
    pub fn sessions(&self) -> impl Iterator<Item = (u32, &Arc<SubscriptionLog>)> {
        self.sessions.iter().map(|(sid, s)| (*sid, &s.log))
    }

    /// Run one tick: drain incoming UDP into per-session sublogs,
    /// detect gaps, fire due NAKs, send periodic SMs. Returns
    /// aggregated [`TickStats`] across all sessions (callers that
    /// want per-session breakdown should use the cumulative
    /// `Counters`).
    pub fn tick(&mut self) -> TickStats {
        let mut stats = TickStats::default();
        self.drain_recv(&mut stats);
        // Per-session bookkeeping. Two passes (gap detect then
        // fire/SM) so we don't read-while-write on pending_naks.
        let now = Instant::now();
        let nak_min = self.config.nak_backoff_min;
        let nak_jitter = self.config.nak_backoff_jitter;
        let sm_interval = self.config.sm_interval;
        let receiver_id = self.config.receiver_id;
        let stream_id = self.config.stream_id;
        let counters = self.counters.as_ref().cloned();

        for (session_id, session) in self.sessions.iter_mut() {
            session.detect_and_schedule_gap(now, nak_min, nak_jitter, counters.as_deref());
            session.fire_due_naks(&self.transport, &mut stats, now, *session_id, stream_id);
            session.maybe_send_sm(
                &self.transport,
                &mut stats,
                now,
                sm_interval,
                stream_id,
                *session_id,
                receiver_id,
            );
        }

        if let Some(c) = &self.counters {
            crate::receiver::fold_into_counters(c, &stats);
        }
        stats
    }

    /// Drain all sessions' SubscriptionLogs. The callback is
    /// invoked once per accepted Data fragment with the frame's
    /// `session_id`, the source addr of the publisher (for callers
    /// that want to verify it didn't change), and a `FrameView`
    /// over the payload. Returns total bytes consumed across
    /// sessions.
    ///
    /// Sessions don't disappear during the callback (we hold
    /// `&self`). To remove a session in response to a payload, the
    /// caller buffers the session_id and calls [`evict`] after
    /// the poll returns.
    pub fn poll<F>(&self, max_bytes_per_session: u32, mut callback: F) -> u32
    where
        F: FnMut(u32, SocketAddr, FrameView<'_>),
    {
        let mut total = 0u32;
        for (&session_id, session) in &self.sessions {
            let dst = session.effective_dst;
            let consumed = session.log.poll(max_bytes_per_session, |view| {
                callback(session_id, dst, view);
            });
            total = total.saturating_add(consumed);
        }
        total
    }

    fn drain_recv(&mut self, stats: &mut TickStats) {
        // Header fields extracted from a frame so we can drop the
        // recv_buf borrow before calling &mut self methods. None
        // means "drop the frame" (parse error, wrong stream, NAK/SM).
        enum FrameKind {
            Data {
                session_id: u32,
                term_id: u32,
                term_offset: u32,
            },
            Setup {
                session_id: u32,
            },
            Heartbeat {
                session_id: u32,
            },
            Drop,
        }

        for _ in 0..self.config.max_recv_per_tick {
            let (from, len) = {
                let buf = self.recv_buf.slice_mut();
                match self.transport.recv_from(buf) {
                    Ok(Some(x)) => x,
                    Ok(None) => return,
                    Err(_) => {
                        stats.recv_errors += 1;
                        continue;
                    }
                }
            };
            stats.bytes_received += len as u64;

            let cfg_stream = self.config.stream_id;
            let kind = {
                let bytes = &self.recv_buf.slice()[..len];
                match parse_frame(bytes) {
                    Ok(FrameView::Data { header, .. }) => {
                        if header.stream_id != cfg_stream {
                            FrameKind::Drop
                        } else {
                            FrameKind::Data {
                                session_id: header.session_id,
                                term_id: header.term_id,
                                term_offset: header.term_offset,
                            }
                        }
                    }
                    Ok(FrameView::Setup(s)) => {
                        if s.stream_id != cfg_stream {
                            FrameKind::Drop
                        } else {
                            FrameKind::Setup {
                                session_id: s.session_id,
                            }
                        }
                    }
                    Ok(FrameView::Heartbeat(h)) => {
                        if h.stream_id != cfg_stream {
                            FrameKind::Drop
                        } else {
                            FrameKind::Heartbeat {
                                session_id: h.session_id,
                            }
                        }
                    }
                    // NAK / SM are sender-bound — drop here.
                    Ok(_) => FrameKind::Drop,
                    Err(ParseError::Misaligned) | Err(_) => FrameKind::Drop,
                }
            };

            let now = Instant::now();
            match kind {
                FrameKind::Drop => {
                    stats.control_drops += 1;
                }
                FrameKind::Data {
                    session_id,
                    term_id,
                    term_offset,
                } => {
                    // Look up / create the session, clone out the
                    // SubscriptionLog Arc so we can call on_fragment
                    // without holding the &mut self borrow.
                    let log = match self.get_or_create_session(session_id, from, stats) {
                        Some(s) => {
                            s.last_publisher_seen_at = Some(now);
                            s.effective_dst = from;
                            Arc::clone(&s.log)
                        }
                        None => continue,
                    };
                    let bytes = &self.recv_buf.slice()[..len];
                    match log.on_fragment(term_id, term_offset, bytes) {
                        AcceptResult::Accepted => stats.fragments_accepted += 1,
                        _ => stats.fragments_dropped += 1,
                    }
                }
                FrameKind::Setup { session_id } => {
                    if let Some(s) = self.get_or_create_session(session_id, from, stats) {
                        s.last_publisher_seen_at = Some(now);
                        s.effective_dst = from;
                        stats.setups_received += 1;
                    }
                }
                FrameKind::Heartbeat { session_id } => {
                    if let Some(s) = self.get_or_create_session(session_id, from, stats) {
                        s.last_publisher_seen_at = Some(now);
                        s.effective_dst = from;
                        stats.heartbeats_received += 1;
                    }
                }
            }
        }
    }

    /// Look up an existing session or allocate a new one. Returns
    /// `None` and bumps `sessions_rejected` if the session would
    /// be new but `max_sessions` is reached.
    fn get_or_create_session(
        &mut self,
        session_id: u32,
        from: SocketAddr,
        stats: &mut TickStats,
    ) -> Option<&mut SessionInbound> {
        // Borrow check note: we can't use `entry` with the
        // counter-bump path easily (the closure borrows self), so
        // do an explicit contains_key + insert.
        if !self.sessions.contains_key(&session_id) {
            if self.sessions.len() as u32 >= self.config.max_sessions {
                stats.control_drops += 1;
                if let Some(c) = &self.counters {
                    c.sessions_rejected
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                return None;
            }
            let log = match SubscriptionLog::new(SubscriptionConfig {
                session_id,
                stream_id: self.config.stream_id,
                initial_term_id: self.config.initial_term_id,
                term_length: self.config.term_length,
            }) {
                Ok(l) => Arc::new(l),
                Err(_) => {
                    // Should be caught at config-validation time,
                    // but if a future term_length change slips
                    // through, surface as a counter rather than
                    // crash the receiver.
                    stats.control_drops += 1;
                    return None;
                }
            };
            self.sessions.insert(
                session_id,
                SessionInbound {
                    log,
                    pending_naks: HashMap::new(),
                    last_sm_at: Instant::now(),
                    last_publisher_seen_at: None,
                    effective_dst: from,
                    rng: Rng::new(session_id as u64 ^ self.config.receiver_id),
                },
            );
            if let Some(c) = &self.counters {
                c.sessions_created
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        Some(self.sessions.get_mut(&session_id).expect("just inserted"))
    }
}

impl SessionInbound {
    fn detect_and_schedule_gap(
        &mut self,
        now: Instant,
        nak_min: Duration,
        nak_jitter: Duration,
        counters: Option<&Counters>,
    ) {
        let bits = self.log.term_length_bits();
        let sub_pos = self.log.subscriber_position();
        let sub_term_id = (sub_pos >> bits) as u32;
        let sub_offset = (sub_pos & ((1u64 << bits) - 1)) as u32;

        let Some(partition) = (0..3).find(|&p| self.log.partition_term_id(p) == sub_term_id) else {
            return;
        };

        let hwm = self.log.partition_high_water_mark(partition);
        if hwm <= sub_offset {
            return;
        }

        let gap_length = hwm - sub_offset;
        let key = (sub_term_id, sub_offset, gap_length);
        if self.pending_naks.contains_key(&key) {
            return;
        }

        if let Some(c) = counters {
            c.gaps_detected
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            c.bytes_in_gaps
                .fetch_add(gap_length as u64, std::sync::atomic::Ordering::Relaxed);
        }
        // Per-gap loss callback (the single-session receiver
        // exposes `set_loss_callback`) is intentionally not wired
        // here — the muxed variant defers it until a concrete
        // need shows up. Most consumers only care about the
        // aggregated counters.

        let jitter_us = self.rng.in_range(nak_jitter.as_micros().max(1) as u64);
        let fire_at = now + nak_min + Duration::from_micros(jitter_us);
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

    fn fire_due_naks<T: UdpTransport>(
        &mut self,
        transport: &T,
        stats: &mut TickStats,
        now: Instant,
        session_id: u32,
        stream_id: u32,
    ) {
        let bits = self.log.term_length_bits();
        let sub_pos = self.log.subscriber_position();

        let mut to_remove: Vec<(u32, u32, u32)> = Vec::new();
        for (&key, pending) in &self.pending_naks {
            if pending.fire_at > now {
                continue;
            }
            let still_valid = self.gap_still_present(
                sub_pos,
                bits,
                pending.term_id,
                pending.term_offset,
                pending.gap_length,
            );
            if !still_valid {
                stats.naks_suppressed += 1;
                to_remove.push(key);
                continue;
            }

            let nak = NakFrame::new(
                session_id,
                stream_id,
                pending.term_id,
                pending.term_offset,
                pending.gap_length,
            );
            match transport.send_to(self.effective_dst, bytemuck::bytes_of(&nak)) {
                Ok(_) => stats.naks_sent += 1,
                Err(_) => stats.send_errors += 1,
            }
            to_remove.push(key);
        }
        for k in to_remove {
            self.pending_naks.remove(&k);
        }
    }

    fn gap_still_present(
        &self,
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
        let Some(partition) = (0..3).find(|&p| self.log.partition_term_id(p) == term_id) else {
            return false;
        };
        if sub_pos > gap_start {
            return false;
        }
        let hwm = self.log.partition_high_water_mark(partition);
        hwm > term_offset
    }

    #[allow(clippy::too_many_arguments)]
    fn maybe_send_sm<T: UdpTransport>(
        &mut self,
        transport: &T,
        stats: &mut TickStats,
        now: Instant,
        sm_interval: Duration,
        stream_id: u32,
        session_id: u32,
        receiver_id: u64,
    ) {
        if now.duration_since(self.last_sm_at) < sm_interval {
            return;
        }
        let bits = self.log.term_length_bits();
        let sub_pos = self.log.subscriber_position();
        let term_id = (sub_pos >> bits) as u32;
        let offset = (sub_pos & ((1u64 << bits) - 1)) as u32;
        let term_length = self.log.config().term_length;
        let window = 3u32.saturating_mul(term_length).saturating_sub(offset);
        let sm = StatusMessage::new(session_id, stream_id, term_id, offset, window, receiver_id);
        match transport.send_to(self.effective_dst, bytemuck::bytes_of(&sm)) {
            Ok(_) => {
                stats.sms_sent += 1;
                self.last_sm_at = now;
            }
            Err(_) => stats.send_errors += 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::KernelUdp;
    use crate::wire::{DataFrame, HeartbeatFrame, SetupFrame, data_flags};
    use std::net::{IpAddr, Ipv4Addr};

    const STREAM_ID: u32 = 11;
    const INITIAL_TERM_ID: u32 = 100;
    const TERM_LENGTH: u32 = 64 * 1024;

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    fn cfg() -> MuxedReceiverConfig {
        let mut c = MuxedReceiverConfig::defaults(42);
        c.stream_id = STREAM_ID;
        c.initial_term_id = INITIAL_TERM_ID;
        c.term_length = TERM_LENGTH;
        c.sm_interval = Duration::from_micros(100);
        c.nak_backoff_min = Duration::from_micros(50);
        c.nak_backoff_jitter = Duration::from_micros(50);
        c
    }

    fn build_data(session_id: u32, term_id: u32, term_offset: u32, payload: &[u8]) -> Vec<u8> {
        let header = DataFrame::new(
            session_id,
            STREAM_ID,
            term_id,
            term_offset,
            data_flags::UNFRAGMENTED,
            payload.len() as u32,
        );
        let mut buf = Vec::with_capacity(DataFrame::HEADER_LEN + payload.len());
        buf.extend_from_slice(bytemuck::bytes_of(&header));
        buf.extend_from_slice(payload);
        buf
    }

    fn build_recv() -> (MuxedReceiver<KernelUdp>, KernelUdp, SocketAddr) {
        let recv_socket = KernelUdp::bind(loopback(0)).unwrap();
        let recv_addr = recv_socket.local_addr().unwrap();
        let publisher = KernelUdp::bind(loopback(0)).unwrap();
        let receiver = MuxedReceiver::new(recv_socket, cfg());
        (receiver, publisher, recv_addr)
    }

    #[test]
    fn unknown_session_lazily_allocated_on_first_data_frame() {
        let (mut receiver, publisher, recv_addr) = build_recv();
        assert_eq!(receiver.session_count(), 0);

        let frag = build_data(/*session_id*/ 7, INITIAL_TERM_ID, 0, b"hello");
        publisher.send_to(recv_addr, &frag).unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && receiver.session_count() == 0 {
            receiver.tick();
        }
        assert_eq!(receiver.session_count(), 1);
        assert_eq!(
            receiver.effective_dst(7),
            Some(publisher.local_addr().unwrap()),
        );
    }

    #[test]
    fn two_sessions_routed_independently() {
        // Two publishers send under distinct session_ids — the
        // muxer must allocate two SubscriptionLogs and feed each
        // its own fragments.
        let (mut receiver, pub_a, recv_addr) = build_recv();
        let pub_b = KernelUdp::bind(loopback(0)).unwrap();

        let frag_a = build_data(/*session*/ 1, INITIAL_TERM_ID, 0, b"AAAA");
        let frag_b = build_data(/*session*/ 2, INITIAL_TERM_ID, 0, b"BBBB");
        pub_a.send_to(recv_addr, &frag_a).unwrap();
        pub_b.send_to(recv_addr, &frag_b).unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && receiver.session_count() < 2 {
            receiver.tick();
        }
        assert_eq!(receiver.session_count(), 2);

        // Drain via poll and confirm we see one Data frame per
        // session, with payloads matched to their session.
        let mut delivered: Vec<(u32, Vec<u8>)> = Vec::new();
        receiver.poll(64 * 1024, |sid, _src, view| {
            if let FrameView::Data { payload, .. } = view {
                delivered.push((sid, payload.to_vec()));
            }
        });
        delivered.sort_by_key(|(sid, _)| *sid);
        assert_eq!(delivered.len(), 2);
        assert_eq!(delivered[0], (1, b"AAAA".to_vec()));
        assert_eq!(delivered[1], (2, b"BBBB".to_vec()));
    }

    #[test]
    fn frame_for_wrong_stream_dropped_without_allocating_session() {
        // Session-routing must NOT key off frames whose stream_id
        // doesn't match — otherwise an attacker could exhaust
        // max_sessions by spraying random stream_ids.
        let (mut receiver, publisher, recv_addr) = build_recv();
        let bad_header = DataFrame::new(
            /*session*/ 99,
            /*stream*/ STREAM_ID + 1, // wrong stream
            INITIAL_TERM_ID,
            0,
            data_flags::UNFRAGMENTED,
            4,
        );
        let mut buf = Vec::with_capacity(DataFrame::HEADER_LEN + 4);
        buf.extend_from_slice(bytemuck::bytes_of(&bad_header));
        buf.extend_from_slice(b"data");
        publisher.send_to(recv_addr, &buf).unwrap();

        // Tick a few times — the frame should be drained and dropped,
        // but no session created.
        for _ in 0..16 {
            receiver.tick();
        }
        assert_eq!(receiver.session_count(), 0);
    }

    #[test]
    fn max_sessions_cap_rejects_overflow() {
        let recv_socket = KernelUdp::bind(loopback(0)).unwrap();
        let recv_addr = recv_socket.local_addr().unwrap();
        let mut config = cfg();
        config.max_sessions = 2;
        let mut receiver = MuxedReceiver::new(recv_socket, config);

        let publisher = KernelUdp::bind(loopback(0)).unwrap();
        for sid in [11, 22, 33] {
            let frag = build_data(sid, INITIAL_TERM_ID, 0, b"x");
            publisher.send_to(recv_addr, &frag).unwrap();
        }

        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && receiver.session_count() < 2 {
            receiver.tick();
        }
        // Drain any straggler frames so the third session_id (33)
        // has actually been seen and rejected, not just queued.
        for _ in 0..16 {
            receiver.tick();
        }
        assert_eq!(receiver.session_count(), 2);
        assert!(receiver.effective_dst(33).is_none());
    }

    #[test]
    fn evict_removes_session_state() {
        let (mut receiver, publisher, recv_addr) = build_recv();
        let frag = build_data(5, INITIAL_TERM_ID, 0, b"x");
        publisher.send_to(recv_addr, &frag).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && receiver.session_count() == 0 {
            receiver.tick();
        }
        assert_eq!(receiver.session_count(), 1);

        receiver.evict(5);
        assert_eq!(receiver.session_count(), 0);
        assert!(receiver.effective_dst(5).is_none());

        // Evicting a missing session is a no-op.
        receiver.evict(5);
        assert_eq!(receiver.session_count(), 0);
    }

    #[test]
    fn periodic_sm_targets_the_session_publisher_addr() {
        // Two publishers under distinct session_ids — each must
        // get its own SM, NOT cross-routed. Locks down the
        // multi-session SM-targeting property.
        let recv_socket = KernelUdp::bind(loopback(0)).unwrap();
        let recv_addr = recv_socket.local_addr().unwrap();
        let mut receiver = MuxedReceiver::new(recv_socket, cfg());

        let pub_a = KernelUdp::bind(loopback(0)).unwrap();
        let addr_a = pub_a.local_addr().unwrap();
        let pub_b = KernelUdp::bind(loopback(0)).unwrap();
        let addr_b = pub_b.local_addr().unwrap();

        // Use Heartbeat for the kick so we're not also testing
        // payload delivery.
        let hb_a = HeartbeatFrame::new(/*session*/ 100, STREAM_ID);
        let hb_b = HeartbeatFrame::new(/*session*/ 200, STREAM_ID);
        pub_a.send_to(recv_addr, bytemuck::bytes_of(&hb_a)).unwrap();
        pub_b.send_to(recv_addr, bytemuck::bytes_of(&hb_b)).unwrap();

        // Sleep past sm_interval (100µs) and tick — each session's
        // first SM should fire.
        std::thread::sleep(Duration::from_millis(2));
        let mut total_sms = 0u32;
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && total_sms < 2 {
            let s = receiver.tick();
            total_sms += s.sms_sent;
        }
        assert!(total_sms >= 2, "got {total_sms} SMs total");

        // Drain pub_a's socket: it should see exactly an SM with
        // session_id=100.
        let mut buf = [0u8; 2048];
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut got_a = false;
        while !got_a && Instant::now() < deadline {
            if let Some((_, n)) = pub_a.recv_from(&mut buf).unwrap() {
                if let Ok(FrameView::StatusMessage(sm)) = parse_frame(&buf[..n])
                    && sm.session_id == 100
                {
                    got_a = true;
                }
            } else {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        assert!(got_a, "pub_a never received its SM (session_id=100)");

        // pub_b: session_id=200.
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut got_b = false;
        while !got_b && Instant::now() < deadline {
            if let Some((_, n)) = pub_b.recv_from(&mut buf).unwrap() {
                if let Ok(FrameView::StatusMessage(sm)) = parse_frame(&buf[..n])
                    && sm.session_id == 200
                {
                    got_b = true;
                }
            } else {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        assert!(got_b, "pub_b never received its SM (session_id=200)");

        // Don't assert nothing-on-the-other socket — Setups, NAKs,
        // and heartbeats may flow as well; the test asserts each
        // publisher saw ITS SM, which is the correctness property.
        let _ = addr_a;
        let _ = addr_b;
    }

    #[test]
    fn setup_and_heartbeat_also_create_sessions() {
        // A subscriber that joins late (sees only Setup or
        // Heartbeat first, before any Data) must still allocate
        // session state so the session can SM back and pull the
        // initial Data via NAK.
        let (mut receiver, publisher, recv_addr) = build_recv();
        let setup = SetupFrame::new(
            /*session_id*/ 1,
            STREAM_ID,
            /*initial_term_id*/ INITIAL_TERM_ID,
            /*active_term_id*/ INITIAL_TERM_ID,
            /*term_offset*/ 0,
            /*term_length*/ TERM_LENGTH,
        );
        publisher
            .send_to(recv_addr, bytemuck::bytes_of(&setup))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && receiver.session_count() == 0 {
            receiver.tick();
        }
        assert_eq!(receiver.session_count(), 1);
        assert_eq!(
            receiver.effective_dst(1),
            Some(publisher.local_addr().unwrap()),
        );
    }

    #[test]
    fn nak_fires_for_a_real_gap_on_a_per_session_basis() {
        // The single-session receiver has thorough NAK tests; the
        // muxed variant copies the algorithm, so this is a smaller
        // sanity test that confirms (a) a gap on session A produces
        // a NAK, and (b) the NAK is targeted at session A's source
        // addr — NOT at session B's addr (which would indicate
        // cross-session leakage).
        let recv_socket = KernelUdp::bind(loopback(0)).unwrap();
        let recv_addr = recv_socket.local_addr().unwrap();
        let mut config = cfg();
        // Tighten backoff so the NAK fires quickly under the test's
        // polling cadence.
        config.nak_backoff_min = Duration::from_micros(50);
        config.nak_backoff_jitter = Duration::from_micros(50);
        let mut receiver = MuxedReceiver::new(recv_socket, config);

        let pub_a = KernelUdp::bind(loopback(0)).unwrap();
        let addr_a = pub_a.local_addr().unwrap();
        let pub_b = KernelUdp::bind(loopback(0)).unwrap();
        let _addr_b = pub_b.local_addr().unwrap();

        // Session A sends fragment #2 (offset 96) only — leaves a
        // gap at offset 0..96 that should trigger a NAK back to A.
        // The fragment uses the default fragment alignment (32B);
        // sending at offset 96 simulates the first 96 bytes (3
        // fragments) being lost.
        let frag = build_data(/*session*/ 1, INITIAL_TERM_ID, 96, &[0xC9u8; 64]);
        pub_a.send_to(recv_addr, &frag).unwrap();
        // Session B sends a normal first fragment so the muxer
        // creates state for it too — purely so we can assert B
        // does NOT receive A's NAK.
        let frag_b = build_data(/*session*/ 2, INITIAL_TERM_ID, 0, b"x");
        pub_b.send_to(recv_addr, &frag_b).unwrap();

        // Tick until at least one NAK has been sent. tick() returns
        // aggregated stats across sessions.
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut total_naks = 0u32;
        while Instant::now() < deadline && total_naks == 0 {
            let s = receiver.tick();
            total_naks += s.naks_sent;
        }
        assert!(total_naks >= 1, "no NAK fired within deadline");

        // pub_a must receive a NAK frame addressed to its socket.
        let mut buf = [0u8; 2048];
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut got_nak_a = false;
        while !got_nak_a && Instant::now() < deadline {
            if let Some((from, n)) = pub_a.recv_from(&mut buf).unwrap() {
                if let Ok(FrameView::Nak(nak)) = parse_frame(&buf[..n])
                    && nak.session_id == 1
                {
                    got_nak_a = true;
                    // Sanity: the NAK arrived from the receiver's
                    // socket, addressed to pub_a's bound port.
                    let _ = from;
                }
            } else {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        assert!(got_nak_a, "session A's NAK didn't arrive at pub_a");

        // pub_b must NOT receive a NAK with session_id=1 — that
        // would mean cross-session routing leaked. (It MAY receive
        // an SM with session_id=2, which is fine.)
        let _ = addr_a;
        let mut leaked_to_b = false;
        // Brief drain to ensure no straggler NAK addressed to A
        // ended up at B.
        for _ in 0..32 {
            match pub_b.recv_from(&mut buf).unwrap() {
                Some((_, n)) => {
                    if let Ok(FrameView::Nak(nak)) = parse_frame(&buf[..n])
                        && nak.session_id == 1
                    {
                        leaked_to_b = true;
                    }
                }
                None => break,
            }
        }
        assert!(
            !leaked_to_b,
            "session A's NAK leaked to session B's publisher socket",
        );
    }
}
