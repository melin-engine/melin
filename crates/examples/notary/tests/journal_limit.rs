//! What this application costs the journal: entry size, and the fsync
//! batch length that follows from it.
//!
//! `AppEvent::MAX_ENCODED_SIZE` is what the journal reserves per entry,
//! and the transport sizes a batch by how many such entries fit the
//! smaller of the two chunks a batch is copied into — the journal
//! hand-off chunk and the replication chunk. A wide event therefore buys
//! itself a shorter batch: fewer events per fsync. Committing to a fixed
//! 32-byte digest keeps this application at the full batch length, which
//! is the second reason the digest design is worth having, and the one
//! that is easy to lose sight of.

use melin_app::AppEvent;
use melin_journal::codec;
use melin_journal::encoder::{MAX_ENTRY_SIZE, entry_size};
use melin_journal::event::JournalEvent;
use melin_transport_core::pipeline::{MAX_JOURNAL_BATCH, max_journal_batch};
use notary_server::{LEAF_LEN, NotaryEvent};

/// The declared bound must describe reality: the widest event has to
/// encode to exactly `entry_size`. A bound that merely *exceeds* the
/// truth silently shortens every fsync batch.
#[test]
fn widest_event_encodes_to_exactly_the_reserved_entry_size() {
    let event = NotaryEvent::Notarize {
        leaf: [0xA5; LEAF_LEN],
    };
    assert_eq!(event.encoded_size(), NotaryEvent::MAX_ENCODED_SIZE);

    let mut buf = [0u8; MAX_ENTRY_SIZE];
    let written =
        codec::encode(1, 0, 0, 0, &JournalEvent::App(event), &mut buf).expect("encodes cleanly");

    println!(
        "MAX_ENCODED_SIZE={}  entry={written}B  reserved={}B  batch={} of {MAX_JOURNAL_BATCH}",
        NotaryEvent::MAX_ENCODED_SIZE,
        entry_size::<NotaryEvent>(),
        max_journal_batch::<NotaryEvent>(),
    );

    assert_eq!(written, entry_size::<NotaryEvent>());
}

/// Every variant must fit the reservation, not just the widest.
#[test]
fn every_variant_fits_the_reserved_entry_size() {
    for event in [
        NotaryEvent::Notarize {
            leaf: [0; LEAF_LEN],
        },
        NotaryEvent::GetHead,
    ] {
        let mut buf = [0u8; MAX_ENTRY_SIZE];
        let written = codec::encode(1, 0, 0, 0, &JournalEvent::App(event), &mut buf)
            .expect("every variant encodes");
        assert!(
            written <= entry_size::<NotaryEvent>(),
            "{event:?} encoded to {written}B, past the reservation"
        );
    }
}

/// The payoff: a digest-sized event keeps the full fsync batch. An
/// application that carried documents inline would trade this away.
#[test]
fn the_journal_keeps_its_full_batch_length() {
    assert_eq!(
        max_journal_batch::<NotaryEvent>(),
        MAX_JOURNAL_BATCH,
        "this application's events are narrow enough that the journal \
         batches the full ceiling — if that stops being true, the event \
         grew and fsync amortisation went with it"
    );
}
