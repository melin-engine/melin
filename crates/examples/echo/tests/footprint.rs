//! What this application costs the pipeline in ring memory.
//!
//! `AppEvent` is `Copy` and the hot path cannot allocate, so an event
//! lives inline in every ring slot — and the rings hold a fixed number of
//! slots, so the event's *bound* multiplies straight through by the ring
//! capacity, whatever a given request actually carries. At a full-sized
//! payload the event is what dominates a slot, and the rings are several
//! times what a digest-sized event would make them. That is the price of
//! the width this example measures the floor at; this test prints it and
//! pins it, so a change to `MAX_PAYLOAD` shows here before it shows on a
//! host's memory.

use echo_server::{EchoReport, MAX_PAYLOAD, Payload};
use melin_transport_core::pipeline::{
    INPUT_RING_CAPACITY, InputSlot, OUTPUT_RING_CAPACITY, OutputSlot,
};

/// Ceiling on the two rings' combined footprint, in bytes.
///
/// An input slot is the pipeline's own per-slot metadata plus the event;
/// an output slot the report plus its metadata, and the report carries
/// the same payload back. Both are the payload plus a few dozen bytes, so
/// the two rings together come to a little over twice the payload times
/// the ring capacity. The budget sits just above that: enough for
/// slot-layout drift, tight enough that growing the payload fails here.
const RING_BUDGET_BYTES: usize = 768 * 1024 * 1024;

#[test]
fn ring_footprint_is_within_budget() {
    let input_slot = size_of::<InputSlot<Payload>>();
    let output_slot = size_of::<OutputSlot<EchoReport, ()>>();

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
fn the_event_is_barely_larger_than_the_payload_it_can_carry() {
    let event = size_of::<Payload>();
    assert!(
        event <= MAX_PAYLOAD + 8,
        "Payload is {event} bytes to carry at most {MAX_PAYLOAD} — something has \
         been added that the ring capacity will multiply"
    );
}
