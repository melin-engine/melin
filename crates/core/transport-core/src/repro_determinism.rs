//! Failing reproductions for findings 1, 5, 6, 7 and 16 of
//! `docs/internal/determinism-audit-2026-09.md`. Gated behind the
//! `determinism-repro` feature. Every test asserts the documented
//! behaviour; a failure is the bug reproducing, and its message says what
//! actually happened.

use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use melin_journal::{BufferedWriter, JournalEvent, JournalReader};
use melin_pipeline::wait::WaitStrategy;

use crate::journaled_app::JournaledApp;
use crate::pipeline::{InputSlot, MAX_JOURNAL_BATCH, StageWaits, build_replica_pipeline};
use crate::test_support::{TestApp, TestEvent};

type Writer = BufferedWriter<TestEvent>;
type Ja = JournaledApp<TestApp, Writer>;

/// A fresh segment holding one `Add(seq)` entry per `seq`, submitted under
/// key 1, written and fsynced as one batch.
fn write_entries(path: &Path, seqs: &[u64]) {
    let mut w = Writer::create(path).unwrap();
    for &s in seqs {
        w.encode_event(s, 1_000 * s, &JournalEvent::App(TestEvent::Add(s)), 1)
            .unwrap();
    }
    w.flush_batch_sync().unwrap();
}

/// `ends[0]` is where the first entry starts; `ends[k]` is just past entry k.
fn entry_ends(path: &Path) -> Vec<u64> {
    let mut r = JournalReader::<TestEvent>::open(path).unwrap();
    let mut ends = vec![r.valid_file_end()];
    while r.next_entry().unwrap().is_some() {
        ends.push(r.valid_file_end());
    }
    ends
}

fn read_seqs(path: &Path) -> String {
    let mut r = match JournalReader::<TestEvent>::open(path) {
        Ok(r) => r,
        Err(e) => return format!("open failed: {e}"),
    };
    let mut seqs = Vec::new();
    loop {
        match r.next_entry() {
            Ok(Some(e)) => seqs.push(e.sequence),
            Ok(None) => return format!("{seqs:?}"),
            Err(e) => return format!("{seqs:?} then error: {e}"),
        }
    }
}

fn patch(path: &Path, offset: u64, bytes: &[u8]) {
    let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.write_all_at(bytes, offset).unwrap();
    f.sync_all().unwrap();
}

fn recover_outcome(path: &Path) -> Result<u64, String> {
    Ja::recover(TestApp::new(), path)
        .map(|ja| ja.app().total)
        .map_err(|e| e.to_string())
}

/// Finding 5. docs/journal.md: "A CRC mismatch or sequence gap mid-stream
/// is treated as real corruption and returns an error." In the live
/// segment a gap is instead taken for a torn tail, and `open_append` then
/// cuts the file at the gap, destroying the durable entries after it.
#[test]
fn live_segment_gap_is_not_silently_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("gap.journal");
    write_entries(&path, &[1, 2, 3, 5, 6]);

    let outcome = recover_outcome(&path);
    let left = read_seqs(&path);
    assert!(
        outcome.is_err(),
        "recovery accepted a gapped live segment: total={outcome:?}; durable entries 5 and 6 \
         were cut from the file, which now holds {left}"
    );
}

/// Finding 6. A zeroed range starting on an entry boundary, with durable
/// entries after it. The zero-CRC path refuses this shape as data loss;
/// the zero-magic path reads it as end-of-data.
#[test]
fn zeroed_range_mid_segment_is_not_read_as_end_of_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hole.journal");
    write_entries(&path, &(1..=10).collect::<Vec<_>>());
    let ends = entry_ends(&path);
    let (from, to) = (ends[3], ends[6]);
    patch(&path, from, &vec![0u8; (to - from) as usize]);

    let outcome = recover_outcome(&path);
    let left = read_seqs(&path);
    assert!(
        outcome.is_err(),
        "recovery accepted a hole over entries 4..=6: total={outcome:?}; durable entries 7..=10 \
         were cut from the file, which now holds {left}"
    );
}

/// Finding 7. CRC32C is promised to catch bit rot. One flipped bit in the
/// last entry's `length` moves its CRC slot into the preallocated zeros,
/// and the zero-CRC tail heuristic then drops the entry without an error.
#[test]
fn bit_flip_in_last_entry_length_is_detected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("flip.journal");
    write_entries(&path, &[1, 2, 3, 4, 5]);
    let ends = entry_ends(&path);
    let len_off = ends[4] + 2;
    let f = std::fs::File::open(&path).unwrap();
    let mut len = [0u8; 2];
    f.read_exact_at(&mut len, len_off).unwrap();
    let flipped = u16::from_le_bytes(len) ^ (1 << 6);
    patch(&path, len_off, &flipped.to_le_bytes());

    let outcome = recover_outcome(&path);
    let left = read_seqs(&path);
    assert!(
        outcome.is_err(),
        "recovery accepted a bit flip in entry 5's length: total={outcome:?} (1+2+3+4+5=15 was \
         durable); the file now holds {left}"
    );
}

/// Finding 16. A crash in the middle of writing entry 6, which was never
/// acknowledged, leaves a prefix of its bytes in front of preallocation
/// zeros. Recovery must discard that prefix. At some cut points it
/// refuses to start.
#[test]
fn torn_unacked_final_entry_never_blocks_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let full = dir.path().join("full.journal");
    write_entries(&full, &[1, 2, 3, 4, 5, 6]);
    let ends = entry_ends(&full);
    let mut entry6 = vec![0u8; (ends[6] - ends[5]) as usize];
    std::fs::File::open(&full)
        .unwrap()
        .read_exact_at(&mut entry6, ends[5])
        .unwrap();

    let mut refused = Vec::new();
    for cut in 1..entry6.len() {
        let path = dir.path().join(format!("torn-{cut}.journal"));
        write_entries(&path, &[1, 2, 3, 4, 5]);
        patch(&path, ends[5], &entry6[..cut]);
        match recover_outcome(&path) {
            Ok(total) => assert_eq!(total, 15, "cut {cut}: acked entries lost"),
            Err(e) => refused.push(format!("cut {cut}/{}: {e}", entry6.len())),
        }
        std::fs::remove_file(&path).unwrap();
    }
    assert!(
        refused.is_empty(),
        "recovery refused to start on a torn, never-acked tail at these cut points:\n{}",
        refused.join("\n")
    );
}

/// Finding 1, root cause. The reconnect handshake reads
/// `(journal_seq, chain_hash)` from this seqlock whenever the replica has
/// a pipeline. It must describe the journal the pipeline was built on.
#[test]
fn replica_handshake_state_reflects_the_recovered_journal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("replica.journal");
    write_entries(&path, &[1, 2, 3]);
    let ja = Ja::recover(TestApp::new(), &path).unwrap();
    let recovered_chain = ja.chain_hash();
    let (app, writer) = ja.into_parts();
    assert_eq!(writer.next_sequence(), 4);

    let pipeline = build_replica_pipeline(
        app,
        writer,
        MAX_JOURNAL_BATCH,
        Duration::ZERO,
        StageWaits::uniform(WaitStrategy::SpinThenYield),
        false,
        Arc::new(crate::fence::FenceState::new(0)),
    );
    assert_eq!(
        pipeline.cursors.durable_wire_seq().load().get(),
        3,
        "the durable cursor is seeded from the writer"
    );
    let fsync = pipeline
        .chain_hash_lock
        .as_ref()
        .expect("replicas always publish FsyncState")
        .load();
    assert_eq!(
        (fsync.journal_seq.get(), Some(fsync.chain_hash)),
        (3, recovered_chain),
        "a reconnect before this pipeline's first durable batch handshakes with this pair, \
         i.e. as a fresh replica at sequence 0 with a zero chain hash"
    );
}

/// Finding 1, consequence. What the replica receiver publishes after a
/// reconnect whose session is anchored below what the pipeline already
/// holds: the primary's entries again, with their original sequences.
/// Nothing downstream refuses them.
#[test]
fn replica_pipeline_refuses_a_restreamed_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("restream.journal");
    write_entries(&path, &[1, 2, 3]);
    let (app, writer) = Ja::recover(TestApp::new(), &path).unwrap().into_parts();
    let total_before = app.total;

    let pipeline = build_replica_pipeline(
        app,
        writer,
        MAX_JOURNAL_BATCH,
        Duration::ZERO,
        StageWaits::uniform(WaitStrategy::SpinThenYield),
        false,
        Arc::new(crate::fence::FenceState::new(0)),
    );
    let mut producer = pipeline.input_producer;
    let journal = pipeline.journal_stage;
    let matching = pipeline.matching_stage;
    let shutdown = Arc::new(AtomicBool::new(false));
    let (s1, s2) = (Arc::clone(&shutdown), Arc::clone(&shutdown));
    let t_journal = std::thread::spawn(move || journal.run(&s1));
    let t_matching = std::thread::spawn(move || matching.run(&s2));

    for s in 1..=3u64 {
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 1,
            sequence: s,
            timestamp_ns: 1_000 * s,
            event: JournalEvent::App(TestEvent::Add(s)),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });
    }
    producer.publish(InputSlot::shutdown_sentinel());

    let journal_outcome = match t_journal.join() {
        Ok(Ok(_)) => "journal stage accepted the backward sequences".to_owned(),
        Ok(Err(e)) => format!("journal stage failed: {e}"),
        Err(_) => "journal stage panicked (debug-only assertion)".to_owned(),
    };
    let app = t_matching.join().unwrap();
    let on_disk = read_seqs(&path);
    assert_eq!(
        app.total, total_before,
        "the matching stage applied the re-streamed prefix a second time; {journal_outcome}; \
         the journal now reads {on_disk}"
    );
}
