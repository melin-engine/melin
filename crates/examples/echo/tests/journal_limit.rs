//! What this application costs the journal: the entry reservation, and
//! the fsync batch length that follows from it.
//!
//! `AppEvent::MAX_ENCODED_SIZE` is what the journal reserves per entry,
//! and the transport sizes a batch by how many such entries fit the
//! smaller of the two chunks a batch is copied into — the journal
//! hand-off chunk and the replication chunk. A wide bound therefore buys
//! itself a shorter batch: fewer events per fsync. At a full-sized
//! payload that is exactly what happens, and the second test pins it as
//! the price of the width rather than as a limit to stay under — the
//! notary, at a 32-byte digest, is the example of staying under it.
//!
//! The bound is what the batch is sized by, but each entry is written at
//! its own `encoded_size`: a short payload costs the disk only its bytes.
//! The last test pins that down, since it is the half of the contract a
//! fixed-width event never exercises.

use echo_server::{MAX_PAYLOAD, Payload};
use melin_app::AppEvent;
use melin_journal::codec;
use melin_journal::encoder::{MAX_ENTRY_SIZE, entry_size};
use melin_journal::event::JournalEvent;
use melin_transport_core::pipeline::{MAX_JOURNAL_BATCH, max_journal_batch};

fn echo_of(len: usize) -> Payload {
    Payload::new(&vec![0xA5; len]).expect("within the cap")
}

/// The declared bound must describe reality: the widest event has to
/// encode to exactly `entry_size`. A bound that merely *exceeds* the
/// truth silently shortens every fsync batch further than the width does.
#[test]
fn widest_event_encodes_to_exactly_the_reserved_entry_size() {
    let event = echo_of(MAX_PAYLOAD);
    assert_eq!(event.encoded_size(), Payload::MAX_ENCODED_SIZE);

    let mut buf = [0u8; MAX_ENTRY_SIZE];
    let written =
        codec::encode(1, 0, 0, 0, &JournalEvent::App(event), &mut buf).expect("encodes cleanly");

    println!(
        "MAX_ENCODED_SIZE={}  entry={written}B  reserved={}B  batch={} of {MAX_JOURNAL_BATCH}",
        Payload::MAX_ENCODED_SIZE,
        entry_size::<Payload>(),
        max_journal_batch::<Payload>(),
    );

    assert_eq!(written, entry_size::<Payload>());
}

/// The price of the width: the batch is shorter than the ceiling, and it
/// is the hand-off chunk, not the ceiling, that bounds it. If this ever
/// passes at the full ceiling, the payload shrank and the example no
/// longer measures the floor at a full-sized message.
#[test]
fn the_width_shortens_the_journal_batch() {
    let batch = max_journal_batch::<Payload>();
    assert!(
        batch < MAX_JOURNAL_BATCH,
        "a {MAX_PAYLOAD}-byte payload is meant to be wide enough to shorten the batch \
         below the {MAX_JOURNAL_BATCH} ceiling, but the batch is {batch}"
    );
    assert!(
        batch * entry_size::<Payload>() <= melin_journal::replication::CHUNK_SIZE,
        "the batch must fit the replication chunk"
    );
    assert!(
        (batch + 1) * entry_size::<Payload>() > melin_journal::replication::CHUNK_SIZE,
        "one more entry would not fit: the chunk is what bounds the batch"
    );
}

/// A short payload is written short: the entry grows by exactly the
/// bytes carried, and never past the reservation.
#[test]
fn an_entry_costs_the_bytes_it_carries() {
    let mut buf = [0u8; MAX_ENTRY_SIZE];
    let empty = codec::encode(1, 0, 0, 0, &JournalEvent::App(echo_of(0)), &mut buf)
        .expect("the empty payload encodes");
    for len in [1, 17, 255, 256, MAX_PAYLOAD] {
        let written = codec::encode(1, 0, 0, 0, &JournalEvent::App(echo_of(len)), &mut buf)
            .expect("every size encodes");
        assert_eq!(written, empty + len, "a {len}-byte payload");
        assert!(written <= entry_size::<Payload>());
    }
}
