//! Catch-up → live-stream handoff narrowing tests.
//!
//! When a replica reconnects, the bulk catch-up pass
//! ([`catch_up_from_journal_with`]) runs with the slot's ring inactive,
//! so the journal stage doesn't publish to it. Entries journaled
//! between the bulk pass's scanner EOF and ring activation land in
//! *neither* the catch-up stream *nor* the ring — only on disk.
//! [`bridge_catchup_to_live`] activates the ring, then re-reads those
//! entries off the disk before draining into the live ring.
//!
//! These tests pin that the bridge recovers the window's entries (the
//! pre-bridge handoff went live directly off the bulk pass and shipped
//! a gapped stream). The bridge *narrows* the window deterministically
//! in the common case; it is not the correctness guarantee — because
//! the journal stage publishes to the ring before flushing to disk, a
//! sub-millisecond residual-vs-flush race can still leave a hole. The
//! receiver's sequence-contiguity gate (`process_streaming_frames`,
//! tested in `melin-server-runtime`) is what makes the handoff correct:
//! it rejects any gap fatally, forcing a reconnect rather than a silent
//! hole. See [`bridge_catchup_to_live`] for the full reasoning.
//!
//! Regression: the 2026-06-07 LAN bench (tcp-dual-repl, ~2.9M orders/s)
//! evicted a slow replica during warmup; on reconnect, catch-up ended
//! at seq 6932800 and live streaming resumed at 6933013 — entries
//! 6932801..=6933012 were never sent, and the replica's journal failed
//! lineage verification with a 212-entry sequence gap.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};

use melin_journal::replication::build_replication_ring;
use melin_journal::{BufferedWriter, JournalEvent, JournalWrite};

use super::catchup::{CatchUpResult, bridge_catchup_to_live, catch_up_from_journal_with};
use crate::pipeline::InputSlot;
use crate::replication_wire::{encode_input_batch, try_decode_input_batch};
use crate::test_support::TestEvent;

/// An `InputSlot` carrying its journal sequence, as the journal stage
/// publishes them to the replication rings on the primary.
fn slot(sequence: u64) -> InputSlot<TestEvent> {
    InputSlot {
        connection_id: 0,
        key_hash: 0,
        request_seq: sequence,
        sequence,
        timestamp_ns: 0,
        event: JournalEvent::App(TestEvent::Add(sequence)),
        publish_ts: Default::default(),
        recv_ts: Default::default(),
    }
}

/// Decode a forwarded byte stream (concatenated length-prefixed
/// `InputBatch` frames — exactly what the replica's receiver parses)
/// into the slot sequences it carries, in wire order.
fn forwarded_sequences(stream: &[u8]) -> Vec<u64> {
    let mut seqs = Vec::new();
    let mut off = 0;
    while off < stream.len() {
        assert!(
            off + 4 <= stream.len(),
            "truncated length prefix at offset {off}"
        );
        let len = u32::from_le_bytes(
            stream[off..off + 4]
                .try_into()
                .expect("bounds checked: 4-byte slice"),
        ) as usize;
        assert!(
            off + 4 + len <= stream.len(),
            "truncated frame at offset {off} (len {len})"
        );
        let payload = &stream[off + 4..off + 4 + len];
        let slots = try_decode_input_batch::<TestEvent>(payload)
            .expect("every forwarded frame must be a valid InputBatch");
        seqs.extend(slots.iter().map(|s| s.sequence));
        off += 4 + len;
    }
    seqs
}

/// The exact interleaving from the bench failure, made deterministic:
///
/// - the journal holds 1..=10 when catch-up runs for a replica whose
///   handshake said `last_sequence = 4` → catch-up forwards 5..=10;
/// - the journal stage appends 11..=12 during the handoff window
///   (slot ring still inactive, so they are journaled but never
///   published to the ring);
/// - the ring's first post-activation chunk carries 13..=14.
///
/// The wire stream the replica sees across the whole handoff must be
/// dense: 5..=14 with nothing missing. Entries 11..=12 are on disk, so
/// the bridge's residual journal pass recovers them before draining
/// into the ring. What it must never do is what the bench showed:
/// silently forward 13..=14 after 10.
#[test]
fn handoff_must_not_skip_entries_journaled_before_ring_activation() {
    let dir = tempfile::tempdir().unwrap();
    let live = dir.path().join("primary.journal");

    // Journal contents at the moment the replica reconnects.
    let mut writer = BufferedWriter::<TestEvent>::create(&live).unwrap();
    for i in 1..=10u64 {
        writer
            .append(&JournalEvent::App(TestEvent::Add(i)))
            .unwrap();
    }

    // Phase 1: journal catch-up for a replica at last_sequence = 4.
    // Every published frame is collected into `forwarded` — the byte
    // stream the replica's receiver would parse.
    let shutdown = AtomicBool::new(false);
    let mut forwarded: Vec<u8> = Vec::new();
    let catchup_end = {
        let mut publish = |buf: &[u8]| -> io::Result<()> {
            forwarded.extend_from_slice(buf);
            Ok(())
        };
        match catch_up_from_journal_with::<TestEvent>(&live, 4, &mut publish, &shutdown).unwrap() {
            CatchUpResult::Ok(end) => end,
            CatchUpResult::NeedSnapshot => panic!("full history on disk — no snapshot needed"),
        }
    };
    assert_eq!(catchup_end, 10, "catch-up must reach the journal tail");

    // Phase 2: the handoff window. The journal stage keeps running and
    // appends 11..=12 — durably on disk, but the slot's ring is still
    // inactive, so they are never published to it.
    for i in 11..=12u64 {
        writer
            .append(&JournalEvent::App(TestEvent::Add(i)))
            .unwrap();
    }

    // Phase 3: ring activation. The first batch published after the
    // active flag flips starts at 13.
    for i in 13..=14u64 {
        writer
            .append(&JournalEvent::App(TestEvent::Add(i)))
            .unwrap();
    }
    // Capacity 8: smallest power of two comfortably above the single
    // chunk this test publishes.
    let (mut producer, mut consumers) = build_replication_ring(1, 8);
    let mut chunk = Vec::new();
    encode_input_batch(&[slot(13), slot(14)], &mut chunk);
    producer.publish(&chunk, 14);
    let mut consumer = consumers.pop().expect("ring built with one consumer");

    // Phase 4: the handoff — the bridge both senders call. It owns the
    // ring activation, runs the residual journal pass that recovers
    // 11..=12 off the disk, and drains into the ring.
    let active_flag = AtomicBool::new(false);
    let sent = {
        let mut publish = |buf: &[u8]| -> io::Result<()> {
            forwarded.extend_from_slice(buf);
            Ok(())
        };
        bridge_catchup_to_live::<TestEvent>(
            &live,
            4,
            catchup_end,
            &active_flag,
            &mut consumer,
            &mut publish,
            &shutdown,
        )
        .expect("bridge must succeed with full history on disk")
    };
    assert!(
        active_flag.load(Ordering::Acquire),
        "the bridge owns ring activation — it must flip the flag before \
         its residual pass (this narrows the catch-up→live window; the \
         receiver's contiguity gate is the actual guarantee)"
    );

    // The invariant: the replica-bound stream is dense from the
    // handshake successor through everything that was handed off.
    let seqs = forwarded_sequences(&forwarded);
    let expected: Vec<u64> = (5..=14).collect();
    assert_eq!(
        seqs, expected,
        "catch-up → live handoff forwarded a gapped stream: entries \
         journaled between the catch-up scanner's EOF and ring \
         activation were dropped from the wire (the 2026-06-07 bench \
         hole, 212 entries wide in production)"
    );
    assert_eq!(
        sent.get(),
        14,
        "the sent high-water must cover everything forwarded, including \
         the drained ring chunk"
    );
}

/// Quiet-handoff control: with no traffic during the window (ring chunk
/// continues exactly at the bulk pass's end), the bridge's residual
/// pass finds nothing and the drain forwards the chunk — no spurious
/// error, no duplicate stream.
#[test]
fn bridge_with_no_window_traffic_is_a_plain_drain() {
    let dir = tempfile::tempdir().unwrap();
    let live = dir.path().join("primary.journal");

    let mut writer = BufferedWriter::<TestEvent>::create(&live).unwrap();
    for i in 1..=10u64 {
        writer
            .append(&JournalEvent::App(TestEvent::Add(i)))
            .unwrap();
    }

    let shutdown = AtomicBool::new(false);
    let mut forwarded: Vec<u8> = Vec::new();
    let catchup_end = {
        let mut publish = |buf: &[u8]| -> io::Result<()> {
            forwarded.extend_from_slice(buf);
            Ok(())
        };
        match catch_up_from_journal_with::<TestEvent>(&live, 4, &mut publish, &shutdown).unwrap() {
            CatchUpResult::Ok(end) => end,
            CatchUpResult::NeedSnapshot => panic!("full history on disk — no snapshot needed"),
        }
    };

    // Ring chunk continues exactly past the catch-up end: 11..=12.
    for i in 11..=12u64 {
        writer
            .append(&JournalEvent::App(TestEvent::Add(i)))
            .unwrap();
    }
    let (mut producer, mut consumers) = build_replication_ring(1, 8);
    let mut chunk = Vec::new();
    encode_input_batch(&[slot(11), slot(12)], &mut chunk);
    producer.publish(&chunk, 12);
    let mut consumer = consumers.pop().expect("ring built with one consumer");

    let active_flag = AtomicBool::new(false);
    let sent = {
        let mut publish = |buf: &[u8]| -> io::Result<()> {
            forwarded.extend_from_slice(buf);
            Ok(())
        };
        bridge_catchup_to_live::<TestEvent>(
            &live,
            4,
            catchup_end,
            &active_flag,
            &mut consumer,
            &mut publish,
            &shutdown,
        )
        .expect("quiet bridge must succeed")
    };

    // The residual pass re-reads 11..=12 from disk, then the drain
    // discards the now-covered ring chunk — dense, no duplicates.
    let seqs = forwarded_sequences(&forwarded);
    let expected: Vec<u64> = (5..=12).collect();
    assert_eq!(seqs, expected, "quiet handoff must stay dense");
    assert_eq!(sent.get(), 12);
}
