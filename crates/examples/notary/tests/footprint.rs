//! What this application costs the pipeline in ring memory.
//!
//! `AppEvent` is `Copy` and the hot path cannot allocate, so an event
//! lives inline in every ring slot — and the rings hold a fixed number of
//! slots, so event width multiplies straight through by the ring
//! capacity. Committing to a fixed 32-byte digest instead of carrying a
//! document keeps that multiplier small, which is the main engineering
//! consequence of the design and therefore worth asserting rather than
//! assuming.

use melin_transport_core::pipeline::{
    INPUT_RING_CAPACITY, InputSlot, OUTPUT_RING_CAPACITY, OutputSlot,
};
use notary_server::{NotaryEvent, NotaryHead, NotaryReport};

/// Ceiling on the two rings' combined footprint, in bytes.
///
/// At a 32-byte leaf the event is no longer what dominates an input slot:
/// an `InputSlot` spends 40 bytes on the pipeline's own per-slot metadata
/// (connection id, key hash, request sequence, journal sequence,
/// timestamp) before the event is counted at all, and shrinking the event
/// further would barely move it — the floor is structural. The output
/// slot is the receipt's: two 32-byte heads, a position and a timestamp
/// make it 120 bytes. The two rings come to ~248 MiB.
///
/// That is the state worth protecting. 320 MiB leaves room for
/// slot-layout drift while still failing loudly if the event grows back
/// into the dominant term, which is what carrying documents inline would
/// do: the same rings came to ~712 MiB at a 288-byte inline payload.
const RING_BUDGET_BYTES: usize = 320 * 1024 * 1024;

#[test]
fn ring_footprint_is_within_budget() {
    let input_slot = size_of::<InputSlot<NotaryEvent>>();
    let output_slot = size_of::<OutputSlot<NotaryReport, NotaryHead>>();

    let input_bytes = input_slot * INPUT_RING_CAPACITY;
    let output_bytes = output_slot * OUTPUT_RING_CAPACITY;
    let total = input_bytes + output_bytes;

    // Printed so `cargo test -- --nocapture` reports the real figures;
    // the assertion only catches regressions past the budget.
    println!(
        "input slot={input_slot}B x {INPUT_RING_CAPACITY} = {}MiB  \
         output slot={output_slot}B x {OUTPUT_RING_CAPACITY} = {}MiB  total={}MiB",
        input_bytes >> 20,
        output_bytes >> 20,
        total >> 20,
    );

    assert!(
        total <= RING_BUDGET_BYTES,
        "ring footprint {}MiB exceeds the {}MiB budget",
        total >> 20,
        RING_BUDGET_BYTES >> 20,
    );
}

#[test]
fn the_event_is_barely_larger_than_the_digest_it_carries() {
    let event = size_of::<NotaryEvent>();
    assert!(
        event <= notary_server::LEAF_LEN + 8,
        "NotaryEvent is {event} bytes to carry a {}-byte leaf — something \
         has been added that the ring capacity will multiply",
        notary_server::LEAF_LEN
    );
}
