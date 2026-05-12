//! Integration-style tests for the pipeline stages against the real
//! `Exchange` matching engine. The pipeline source now lives in
//! `melin-transport-core`; these tests stay here because they need the
//! concrete `Exchange`, its journaling helpers, and the trading-bound
//! type aliases — keeping them engine-side avoids a dev-dependency
//! cycle from `transport-core` back to `melin-engine`.

#![cfg(test)]

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
    use std::time::Duration;

    use melin_disruptor::padding::Sequence;
    use melin_disruptor::ring;
    use melin_journal::trace::trace_ts;

    // Generic pipeline items the tests reach for by their raw form.
    use crate::journal::pipeline::{MAX_JOURNAL_BATCH, build_pipeline_with_replication};
    // Replica wiring is only exercised by hash-chain replication tests.
    #[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
    use crate::journal::pipeline::build_replica_pipeline;
    // Trading-bound concrete aliases for everything the tests
    // construct / pattern-match.
    use crate::exchange::Exchange;
    use crate::journal::replication::REPLICATION_RING_CAPACITY;
    use crate::journal::{
        InputSlot, JournalEvent, JournalStage, JournalWriter, MatchingStage, OutputPayload,
        OutputSlot,
    };
    #[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
    use melin_journal::JournalWriterMode;
    use crate::types::RejectReason;
    use crate::types::*;

    /// Return type for `start_matching_with_halt`:
    /// (input_producer, output_consumer, connected_counter, shutdown, join_handle).
    type MatchingHaltResult = (
        ring::Producer<InputSlot>,
        ring::Consumer<OutputSlot>,
        Arc<AtomicU32>,
        Arc<AtomicBool>,
        std::thread::JoinHandle<Exchange>,
    );

    /// First user-event sequence: 2 with hash-chain (genesis takes 1), 1 without.
    /// Only referenced from journal-reader assertions, which are themselves
    /// gated on `not(no-persist)`.
    #[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
    const FIRST_SEQ: u64 = 2;
    #[cfg(all(not(feature = "hash-chain"), not(feature = "no-persist")))]
    const FIRST_SEQ: u64 = 1;

    fn limit_order(id: u64, account: AccountId, side: Side, price: u64, qty: u64) -> Order {
        Order {
            id: OrderId(id),
            account,
            side,
            order_type: OrderType::Limit {
                price: Price(NonZeroU64::new(price).unwrap()),
                post_only: false,
            },
            time_in_force: TimeInForce::GTC,
            quantity: Quantity(NonZeroU64::new(qty).unwrap()),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        }
    }

    /// Primary path: `slot.sequence == 0` so the JournalStage allocates
    /// sequences from the writer at encode time, in publish order. The
    /// encoded entries must carry consecutive sequences starting from
    /// `FIRST_SEQ`.
    #[test]
    fn journal_stage_allocates_primary_sequences() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pipeline_journal.journal");

        let writer = JournalWriter::create_default(&path).unwrap();

        let (mut producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();

        let consumer = consumers.pop().unwrap();
        let stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        producer.publish(InputSlot {
            connection_id: 1,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 1_000_000_000,
            event: JournalEvent::App(crate::trading_event::TradingEvent::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(1),
                    base: CurrencyId(0),
                    quote: CurrencyId(1),
                },
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });
        producer.publish(InputSlot {
            connection_id: 1,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 1_000_000_001,
            event: JournalEvent::App(crate::trading_event::TradingEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 100_000,
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        let handle = std::thread::spawn(move || stage.run(&shutdown2));

        std::thread::sleep(std::time::Duration::from_millis(50));
        shutdown.store(true, Ordering::Relaxed);
        let _writer = handle.join().unwrap();

        // Verify events were journaled with consecutive sequences starting
        // from FIRST_SEQ — proving the journal stage (not the producer)
        // allocated them.
        #[cfg(not(feature = "no-persist"))]
        {
            let mut reader = crate::journal::JournalReader::open(&path).unwrap();
            let entry1 = reader.next_entry().unwrap().unwrap();
            assert_eq!(entry1.sequence, FIRST_SEQ);
            assert!(matches!(
                entry1.event,
                JournalEvent::App(crate::trading_event::TradingEvent::AddInstrument { .. })
            ));
            let entry2 = reader.next_entry().unwrap().unwrap();
            assert_eq!(entry2.sequence, FIRST_SEQ + 1);
            assert!(matches!(
                entry2.event,
                JournalEvent::App(crate::trading_event::TradingEvent::Deposit { .. })
            ));
            assert!(reader.next_entry().unwrap().is_none());
        }
    }

    /// Regression guard for the production failure mode:
    ///
    ///     error at entry 100001: sequence gap: expected N+1, got N
    ///
    /// reported by `journal_verify` after a dual-replica LAN bench run.
    /// The signature (expected = last + 1, actual = last) is produced by
    /// the reader when an auto-emitted Checkpoint at seq X is followed
    /// by a normal event that re-uses seq X — the Checkpoint is skipped
    /// transparently, advances the reader's internal `last_sequence` to
    /// X, then the duplicate event fails the strict-continuity check.
    ///
    /// This test drives the primary JournalStage across the checkpoint
    /// boundary with nothing but the pipeline plumbing around it. It
    /// does **not** currently reproduce the production failure — that
    /// bug likely requires a condition this unit test doesn't exercise
    /// (real io_uring + CQE timing, network ingress, replication
    /// backpressure, rotation, …). Kept as an invariant guard so any
    /// future regression that does manifest at this layer is caught.
    #[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
    #[test]
    fn primary_journal_sequences_contiguous_across_checkpoint_boundary() {
        use crate::journal::checkpoint_interval;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint_boundary.journal");
        let writer = JournalWriter::create_default(&path).unwrap();

        // Ring capacity: power-of-two large enough to hold every event
        // without the publisher ever blocking on the consumer. This lets
        // the pipeline exercise the full in-flight / auto-emit path.
        // Cross the checkpoint boundary at least twice so any off-by-one
        // around the auto-emit is exercised on both the first and second
        // segment.
        let total: u64 = checkpoint_interval() * 2 + 100;
        let cap = ((total as usize) + MAX_JOURNAL_BATCH).next_power_of_two();
        let (mut producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(cap)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();

        let stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || stage.run(&shutdown2));

        for i in 0..total {
            producer.publish(InputSlot {
                connection_id: 0,
                key_hash: 0,
                request_seq: 0,
                sequence: 0,
                timestamp_ns: 1_000_000_000 + i,
                event: JournalEvent::App(crate::trading_event::TradingEvent::Deposit {
                    account: AccountId((i as u32) + 1),
                    currency: CurrencyId(0),
                    amount: 100,
                }),
                publish_ts: trace_ts(),
                recv_ts: trace_ts(),
            });
        }

        // Give the stage time to drain and fsync every batch.
        std::thread::sleep(std::time::Duration::from_millis(1000));
        shutdown.store(true, Ordering::Relaxed);
        let _writer = handle.join().unwrap();

        // Walk the journal entry-by-entry. The reader enforces strict
        // sequence continuity internally: any gap or duplicate surfaces
        // as `SequenceGap`. Transparent entries (GenesisHash, auto-
        // emitted Checkpoint) are skipped without incrementing `count`
        // but still advance the reader's internal `last_sequence`, so a
        // duplicate-after-checkpoint produces the exact error signature
        // seen in production: `expected N+1, got N`.
        let mut reader = crate::journal::JournalReader::open(&path).unwrap();
        let mut count = 0u64;
        loop {
            match reader.next_entry() {
                Ok(Some(_)) => count += 1,
                Ok(None) => break,
                Err(e) => {
                    panic!(
                        "journal read error after {count} user entries \
                         (last_sequence = {:?}): {e}",
                        reader.last_sequence()
                    );
                }
            }
        }
        assert_eq!(
            count, total,
            "expected all {total} user events to be recoverable from the journal"
        );
    }

    /// End-to-end primary → replica test, mirroring the LAN-bench topology:
    ///
    ///   primary disruptor  ─▶ primary JournalStage ─▶ replication ring
    ///                                                       │
    ///                               relay thread decodes bytes │
    ///                                                       ▼
    ///                                               replica disruptor ─▶ replica JournalStage
    ///
    /// The relay thread is the in-test stand-in for `submit_batch_to_
    /// pipeline` in `crates/server/src/replication/mod.rs`: it decodes
    /// each journal batch shipped to the replication ring and re-
    /// publishes every non-QueryStats entry to the replica's input ring
    /// with the primary's sequence stamped on `slot.sequence`.
    ///
    /// Both journals are then read back and must walk cleanly end-to-end
    /// — no `SequenceGap`, no duplicates — across the checkpoint
    /// boundary.
    #[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
    #[test]
    fn primary_and_replica_journals_contiguous_across_checkpoint_boundary() {
        use crate::journal::checkpoint_interval;

        let dir = tempfile::tempdir().unwrap();
        let primary_path = dir.path().join("primary.journal");
        let replica_path = dir.path().join("replica.journal");

        // Shared genesis hash so the two writers seed identical BLAKE3
        // chains. In production the replica gets this via snapshot
        // transfer; here we hard-code it so the chain-hash divergence
        // check inside the replica's JournalStage doesn't short-circuit
        // the test at the first auto-emitted Checkpoint.
        let shared_genesis = [0xA5u8; 32];

        // -------- primary --------
        let mut primary_exchange = Exchange::new();
        primary_exchange.add_instrument(InstrumentSpec {
            symbol: Symbol(1),
            base: CurrencyId(0),
            quote: CurrencyId(1),
        });
        primary_exchange.deposit(AccountId(1), CurrencyId(1), u64::MAX / 2);
        let primary_writer = JournalWriter::create_continuing(
            JournalWriterMode::default(),
            &primary_path,
            1,
            shared_genesis,
        )
        .unwrap();
        let primary_active_conns = Arc::new(AtomicU64::new(0));
        let mut primary = build_pipeline_with_replication(
            primary_exchange,
            primary_writer,
            Duration::ZERO,
            primary_active_conns,
            true, // replication enabled
            MAX_JOURNAL_BATCH,
            REPLICATION_RING_CAPACITY,
            false,
            false,
            false,
        );

        // -------- replica --------
        let mut replica_exchange = Exchange::new();
        replica_exchange.add_instrument(InstrumentSpec {
            symbol: Symbol(1),
            base: CurrencyId(0),
            quote: CurrencyId(1),
        });
        replica_exchange.deposit(AccountId(1), CurrencyId(1), u64::MAX / 2);
        let replica_writer = JournalWriter::create_continuing(
            JournalWriterMode::default(),
            &replica_path,
            1,
            shared_genesis,
        )
        .unwrap();
        let replica = build_replica_pipeline(
            replica_exchange,
            replica_writer,
            MAX_JOURNAL_BATCH,
            std::time::Duration::ZERO,
            false,
            false,
        );

        // Mark a replica as connected so the primary doesn't halt and
        // its journal stage actually publishes to the replication ring.
        if let Some(ref count) = primary.replicas_connected {
            count.store(1, Ordering::Relaxed);
        }
        if let Some(ref rp) = primary.replication_ring_progress {
            rp.active_flags[0].store(true, Ordering::Relaxed);
        }

        let (mut repl_c0, mut repl_c1) =
            primary.replication_consumers.expect("replication enabled");
        let mut replica_input = replica.input_producer;

        let primary_shutdown = Arc::new(AtomicBool::new(false));
        let replica_shutdown = Arc::new(AtomicBool::new(false));
        let relay_shutdown = Arc::new(AtomicBool::new(false));

        // --- relay thread: pump primary's replication ring -> replica's input ring ---
        let relay_stop = Arc::clone(&relay_shutdown);
        let t_relay = std::thread::spawn(move || {
            loop {
                let mut got_something = false;
                // Ring 0: each chunk is a wire-ready `InputBatch` frame
                // ([length:u32][type:0x21][count:u16][slots...]) produced
                // by the primary's journal stage. Strip the length prefix
                // and decode the payload into `InputSlot`s with the
                // primary's sequence stamped, then publish to the replica's
                // input ring. Mirrors what `tcp_receiver.rs` does after
                // `read_frame`.
                if let Some((_meta, data)) = repl_c0.try_read() {
                    let payload_len =
                        u32::from_le_bytes(data[..4].try_into().expect("4-byte length prefix"))
                            as usize;
                    let payload = &data[4..4 + payload_len];
                    let slots: Vec<InputSlot> =
                        melin_transport_core::replication_wire::try_decode_input_batch(payload)
                            .expect("relay InputBatch decode");
                    for slot in slots {
                        // Skip the primary's auto-emitted Checkpoint
                        // entries: the replica has a chain hash seeded
                        // from its own (test-local) genesis, so passing
                        // the primary's Checkpoint through
                        // verify_primary_checkpoint would always diverge
                        // and kill the replica's JournalStage. The
                        // replica still auto-emits its own Checkpoints
                        // at the same sequence positions.
                        if matches!(slot.event, JournalEvent::Checkpoint { .. }) {
                            continue;
                        }
                        replica_input.publish(InputSlot {
                            connection_id: 0,
                            key_hash: slot.key_hash,
                            request_seq: slot.request_seq,
                            sequence: slot.sequence,
                            timestamp_ns: slot.timestamp_ns,
                            event: slot.event,
                            publish_ts: trace_ts(),
                            recv_ts: trace_ts(),
                        });
                    }
                    repl_c0.commit();
                    got_something = true;
                }
                // Ring 1 (unused in this test — only one "replica" is
                // active). Drain defensively so the ring never fills up.
                if repl_c1.try_read().is_some() {
                    repl_c1.commit();
                    got_something = true;
                }
                if !got_something {
                    if relay_stop.load(Ordering::Relaxed) {
                        return;
                    }
                    std::hint::spin_loop();
                }
            }
        });

        // --- primary + replica pipeline threads ---
        let mut primary_output = primary.output_consumers.pop().unwrap();
        let primary_out_shutdown = Arc::new(AtomicBool::new(false));
        let primary_out_stop = Arc::clone(&primary_out_shutdown);
        let t_primary_out = std::thread::spawn(move || {
            while !primary_out_stop.load(Ordering::Relaxed) {
                if primary_output.try_consume().is_some() {
                    continue;
                }
                std::hint::spin_loop();
            }
        });

        let mut replica_drain = replica.drain_consumer;
        let replica_drain_stop = Arc::new(AtomicBool::new(false));
        let replica_drain_stop2 = Arc::clone(&replica_drain_stop);
        let t_replica_drain = std::thread::spawn(move || {
            while !replica_drain_stop2.load(Ordering::Relaxed) {
                if replica_drain.try_consume().is_some() {
                    continue;
                }
                std::hint::spin_loop();
            }
        });

        let p_j_stop = Arc::clone(&primary_shutdown);
        let p_m_stop = Arc::clone(&primary_shutdown);
        let t_p_journal = std::thread::spawn(move || primary.journal_stage.run(&p_j_stop));
        let t_p_matching = std::thread::spawn(move || primary.matching_stage.run(&p_m_stop));

        let r_j_stop = Arc::clone(&replica_shutdown);
        let r_m_stop = Arc::clone(&replica_shutdown);
        let t_r_journal = std::thread::spawn(move || replica.journal_stage.run(&r_j_stop));
        let t_r_matching = std::thread::spawn(move || replica.matching_stage.run(&r_m_stop));

        // Cross several checkpoint boundaries so any subtle interaction
        // between the primary's auto-emit cadence and the relay/replica
        // encode cadence shows up.
        let total: u64 = checkpoint_interval() * 5 + 250;
        for i in 0..total {
            let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
            primary.input_producer.publish(InputSlot {
                connection_id: 1,
                key_hash: 0,
                request_seq: 0,
                sequence: 0,
                timestamp_ns: 1_000_000_000 + i,
                event: JournalEvent::App(crate::trading_event::TradingEvent::SubmitOrder {
                    symbol: Symbol(1),
                    order: limit_order(i + 1, AccountId(1), side, 100, 1),
                }),
                publish_ts: trace_ts(),
                recv_ts: trace_ts(),
            });
        }

        std::thread::sleep(std::time::Duration::from_millis(3000));

        // Shutdown order: primary pipelines first (flushes replication
        // ring), then relay (so it drains any trailing batches), then
        // replica (so it fully ingests what the relay published).
        primary_shutdown.store(true, Ordering::Relaxed);
        let primary_journal_result = t_p_journal.join().unwrap();
        let _ = t_p_matching.join().unwrap();
        relay_shutdown.store(true, Ordering::Relaxed);
        let _ = t_relay.join();
        // Give the replica a moment to ingest the relayed tail.
        std::thread::sleep(std::time::Duration::from_millis(500));
        replica_shutdown.store(true, Ordering::Relaxed);
        let replica_journal_result = t_r_journal.join().unwrap();
        let _ = t_r_matching.join().unwrap();
        primary_journal_result.expect("primary journal stage must exit cleanly");
        replica_journal_result.expect("replica journal stage must exit cleanly");
        primary_out_shutdown.store(true, Ordering::Relaxed);
        let _ = t_primary_out.join();
        replica_drain_stop.store(true, Ordering::Relaxed);
        let _ = t_replica_drain.join();

        // Walk both journals. Either failing with SequenceGap would
        // match the production failure signature.
        let scan = |label: &str, path: &std::path::Path| -> u64 {
            let mut reader = crate::journal::JournalReader::open(path).unwrap();
            let mut count = 0u64;
            loop {
                match reader.next_entry() {
                    Ok(Some(_)) => count += 1,
                    Ok(None) => break,
                    Err(e) => panic!(
                        "{label} journal read error after {count} user entries \
                         (last_sequence = {:?}): {e}",
                        reader.last_sequence()
                    ),
                }
            }
            count
        };

        let primary_count = scan("primary", &primary_path);
        let replica_count = scan("replica", &replica_path);

        assert_eq!(
            primary_count, total,
            "expected all {total} user events recoverable from the primary journal"
        );
        assert_eq!(
            replica_count, total,
            "expected all {total} user events recoverable from the replica journal"
        );
    }

    /// Verify the JournalStage uses pre-assigned sequences and timestamps
    /// when `InputSlot.sequence != 0` (replica mode). The encoded journal
    /// entries must carry the primary's sequence numbers, not locally
    /// allocated ones.
    #[test]
    fn journal_stage_uses_preassigned_sequences() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("preseq.journal");

        let writer = JournalWriter::create_default(&path).unwrap();

        let (mut producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();

        let consumer = consumers.pop().unwrap();
        let stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        // Publish events with pre-assigned sequences (simulating replica mode).
        // Start at sequence 2: when the hash-chain feature is enabled,
        // SectorWriter::create writes a GenesisHash at sequence 1, so the
        // next expected sequence is 2. The reader enforces strict continuity.
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            sequence: 2,
            timestamp_ns: 1_700_000_000_000_000_000, // fixed timestamp
            event: JournalEvent::App(crate::trading_event::TradingEvent::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(1),
                    base: CurrencyId(0),
                    quote: CurrencyId(1),
                },
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            sequence: 3,
            timestamp_ns: 1_700_000_000_000_000_001,
            event: JournalEvent::App(crate::trading_event::TradingEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(0),
                amount: 500,
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        let handle = std::thread::spawn(move || stage.run(&shutdown2));

        std::thread::sleep(std::time::Duration::from_millis(50));
        shutdown.store(true, Ordering::Relaxed);
        let _writer = handle.join().unwrap();

        // Verify the encoded journal entries carry the pre-assigned sequences
        // and timestamps, not locally allocated ones.
        #[cfg(not(feature = "no-persist"))]
        {
            let mut reader = crate::journal::JournalReader::open(&path).unwrap();

            // The reader auto-skips GenesisHash and Checkpoint entries
            // (transparent to callers), so the first visible entry is
            // AddInstrument at sequence 2.
            let entry1 = reader.next_entry().unwrap().unwrap();
            assert_eq!(entry1.sequence, 2);
            assert_eq!(entry1.timestamp_ns, 1_700_000_000_000_000_000);
            assert!(matches!(
                entry1.event,
                JournalEvent::App(crate::trading_event::TradingEvent::AddInstrument { .. })
            ));

            let entry2 = reader.next_entry().unwrap().unwrap();
            assert_eq!(entry2.sequence, 3);
            assert_eq!(entry2.timestamp_ns, 1_700_000_000_000_000_001);
            assert!(matches!(
                entry2.event,
                JournalEvent::App(crate::trading_event::TradingEvent::Deposit { .. })
            ));

            assert!(reader.next_entry().unwrap().is_none());
        }
    }

    /// Verify that the JournalStage detects divergence when a primary
    /// checkpoint carries a chain hash that doesn't match the replica's.
    /// The stage must return a fatal error, not silently continue.
    #[cfg(feature = "hash-chain")]
    #[test]
    fn divergence_detected_on_checkpoint_hash_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("divergence.journal");

        let writer = JournalWriter::create_default(&path).unwrap();

        let (mut producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();

        let consumer = consumers.pop().unwrap();
        let stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        // Publish a normal event with a pre-assigned sequence.
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            sequence: 100,
            timestamp_ns: 1_000_000_000,
            event: JournalEvent::App(crate::trading_event::TradingEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(0),
                amount: 500,
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        // Publish a checkpoint with a deliberately wrong chain hash.
        // This simulates the primary's checkpoint arriving after the
        // replica encoded the preceding events differently.
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            sequence: 101,
            timestamp_ns: 1_000_000_001,
            event: JournalEvent::Checkpoint {
                chain_hash: [0xFF; 32], // bogus hash — will not match
                events_since_checkpoint: 1,
            },
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        let handle = std::thread::spawn(move || stage.run(&shutdown2));

        // Give the stage time to process both events.
        std::thread::sleep(std::time::Duration::from_millis(100));
        shutdown.store(true, Ordering::Relaxed);
        let result = handle.join().unwrap();

        // The stage must return an error due to the hash mismatch.
        match result {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("divergence detected"),
                    "error should mention divergence: {msg}"
                );
            }
            Ok(_) => panic!("expected divergence error, got Ok"),
        }
    }

    #[test]
    fn matching_stage_processes_events() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(InstrumentSpec {
            symbol: Symbol(1),
            base: CurrencyId(0),
            quote: CurrencyId(1),
        });
        exchange.deposit(AccountId(1), CurrencyId(1), 1_000_000);
        exchange.deposit(AccountId(2), CurrencyId(0), 1_000);

        let (mut input_producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();

        let (output_producer, mut output_consumers) = ring::DisruptorBuilder::<OutputSlot>::new(64)
            .add_consumer()
            .build();
        let mut output_consumer = output_consumers.pop().unwrap();

        // Journal cursor and counters not used in this test — create dummies.
        let dummy_cursor = Arc::new(Sequence::new(AtomicU64::new(0)));
        let events_counter = Arc::new(AtomicU64::new(0));
        let active_conns = Arc::new(AtomicU64::new(0));
        let stage = MatchingStage::new(
            exchange,
            consumer,
            output_producer,
            events_counter,
            dummy_cursor,
            active_conns,
            None, // standalone — no halt check
            false,
        );

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        input_producer.publish(InputSlot {
            connection_id: 42,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 0,
            event: JournalEvent::App(crate::trading_event::TradingEvent::SubmitOrder {
                symbol: Symbol(1),
                order: limit_order(1, AccountId(2), Side::Sell, 100, 50),
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        let handle = std::thread::spawn(move || stage.run(&shutdown2));

        let mut attempts = 0;
        let output = loop {
            if let Some((_, slot)) = output_consumer.try_consume() {
                break slot;
            }
            attempts += 1;
            if attempts > 1_000_000 {
                panic!("timeout waiting for output");
            }
            std::hint::spin_loop();
        };

        assert_eq!(output.connection_id, 42);
        assert_eq!(output.input_seq, 0);
        assert!(matches!(
            output.payload,
            OutputPayload::Report(ExecutionReport::Placed { .. })
        ));
        // The Placed slot now also carries the request terminator —
        // the response stage emits the wire BatchEnd from this flag,
        // saving the separate BatchEnd-payload slot.
        assert!(output.is_last_in_request);

        shutdown.store(true, Ordering::Relaxed);
        let _exchange = handle.join().unwrap();
    }

    #[test]
    fn full_pipeline_journal_and_matching_parallel() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("full_pipeline.journal");

        let mut exchange = Exchange::new();
        exchange.add_instrument(InstrumentSpec {
            symbol: Symbol(1),
            base: CurrencyId(0),
            quote: CurrencyId(1),
        });
        exchange.deposit(AccountId(1), CurrencyId(1), 1_000_000);
        exchange.deposit(AccountId(2), CurrencyId(0), 1_000);

        let writer = JournalWriter::create_default(&path).unwrap();

        let active_conns = Arc::new(AtomicU64::new(0));
        let mut out = build_pipeline_with_replication(
            exchange,
            writer,
            Duration::ZERO,
            active_conns,
            false,
            MAX_JOURNAL_BATCH,
            REPLICATION_RING_CAPACITY,
            false,
            false,
            false,
        );
        let mut input_producer = out.input_producer;
        let journal_stage = out.journal_stage;
        let matching_stage = out.matching_stage;
        let journal_cursor = out.journal_cursor;
        let mut output_consumer = out.output_consumers.pop().unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let s1 = Arc::clone(&shutdown);
        let s2 = Arc::clone(&shutdown);

        let t_journal = std::thread::spawn(move || journal_stage.run(&s1));
        let t_matching = std::thread::spawn(move || matching_stage.run(&s2));

        // Submit an order through the pipeline. Primary-side producers
        // publish `sequence: 0`; the journal stage assigns the sequence
        // at encode time.
        input_producer.publish(InputSlot {
            connection_id: 1,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 1_000_000_000,
            event: JournalEvent::App(crate::trading_event::TradingEvent::SubmitOrder {
                symbol: Symbol(1),
                order: limit_order(1, AccountId(2), Side::Sell, 100, 50),
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        // Wait for the Placed report in the output SPSC.
        let output = loop {
            if let Some((_, slot)) = output_consumer.try_consume() {
                break slot;
            }
            std::hint::spin_loop();
        };

        assert!(matches!(
            output.payload,
            OutputPayload::Report(ExecutionReport::Placed { .. })
        ));
        assert_eq!(output.input_seq, 0);

        // Wait for journal to confirm durability (cursor > input_seq).
        loop {
            let cursor = journal_cursor.get().load(Ordering::Acquire);
            if cursor > output.input_seq {
                break;
            }
            std::hint::spin_loop();
        }

        // Now it's safe to send the response — event is durable.

        shutdown.store(true, Ordering::Relaxed);
        let _writer = t_journal.join().unwrap();
        let _exchange = t_matching.join().unwrap();

        // Verify the event was journaled (only when persistence is enabled).
        #[cfg(not(feature = "no-persist"))]
        {
            let mut reader = crate::journal::JournalReader::open(&path).unwrap();
            let entry = reader.next_entry().unwrap().unwrap();
            assert!(matches!(
                entry.event,
                JournalEvent::App(crate::trading_event::TradingEvent::SubmitOrder { .. })
            ));
        }
    }

    #[test]
    #[cfg(not(feature = "no-persist"))]
    fn journal_stage_sends_replication_batches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repl_pipeline.journal");

        let mut exchange = Exchange::new();
        exchange.add_instrument(InstrumentSpec {
            symbol: Symbol(1),
            base: CurrencyId(0),
            quote: CurrencyId(1),
        });
        exchange.deposit(AccountId(1), CurrencyId(1), 1_000_000);
        exchange.deposit(AccountId(2), CurrencyId(0), 1_000);

        let writer = JournalWriter::create_default(&path).unwrap();

        let active_conns = Arc::new(AtomicU64::new(0));
        let mut out = build_pipeline_with_replication(
            exchange,
            writer,
            Duration::ZERO,
            active_conns,
            true,
            MAX_JOURNAL_BATCH,
            REPLICATION_RING_CAPACITY,
            false,
            false,
            false,
        );
        let mut output_consumer = out.output_consumers.pop().unwrap();

        let (mut repl_consumer, _repl_consumer_2) = out
            .replication_consumers
            .expect("replication should be enabled");

        // Simulate a connected replica so the matching stage doesn't halt
        // and the journal stage publishes to replication rings.
        if let Some(ref count) = out.replicas_connected {
            count.store(1, Ordering::Relaxed);
        }
        if let Some(ref rp) = out.replication_ring_progress {
            rp.active_flags[0].store(true, Ordering::Relaxed);
        }

        let journal_stage = out.journal_stage;
        let matching_stage = out.matching_stage;
        let mut input_producer = out.input_producer;
        let journal_cursor = out.journal_cursor;
        let replication_cursor = out.replication_cursor;

        let shutdown = Arc::new(AtomicBool::new(false));
        let s1 = Arc::clone(&shutdown);
        let s2 = Arc::clone(&shutdown);

        let t_journal = std::thread::spawn(move || journal_stage.run(&s1));
        let t_matching = std::thread::spawn(move || matching_stage.run(&s2));

        // Submit an order through the pipeline. The journal stage will
        // assign the sequence at encode time (primary-side `sequence: 0`).
        input_producer.publish(InputSlot {
            connection_id: 1,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 1_000_000_000,
            event: JournalEvent::App(crate::trading_event::TradingEvent::SubmitOrder {
                symbol: Symbol(1),
                order: limit_order(1, AccountId(2), Side::Sell, 100, 50),
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        // Wait for the Placed report in the output SPSC (matching stage).
        let output = loop {
            if let Some((_, slot)) = output_consumer.try_consume() {
                break slot;
            }
            std::hint::spin_loop();
        };
        assert!(matches!(
            output.payload,
            OutputPayload::Report(ExecutionReport::Placed { .. })
        ));

        // Wait for journal to confirm durability.
        loop {
            let cursor = journal_cursor.get().load(Ordering::Acquire);
            if cursor > output.input_seq {
                break;
            }
            std::hint::spin_loop();
        }

        // The journal stage should have published a replication batch with the
        // exact same bytes it wrote to disk. Spin-wait for it.
        let (repl_meta, repl_data) = loop {
            if let Some((meta, data)) = repl_consumer.try_read() {
                // Copy data out before commit releases the slot.
                let data_copy = data.to_vec();
                repl_consumer.commit();
                break (meta, data_copy);
            }
            std::hint::spin_loop();
        };
        assert!(
            repl_meta.end_sequence > 0,
            "replication batch should have events"
        );
        assert!(!repl_data.is_empty(), "replication batch should have data");

        // Replication chunk is a wire-ready `InputBatch` frame:
        // [length:u32][type:0x21][count:u16][slots...]. Decode it and
        // verify the slot's sequence + event match what we submitted.
        let payload_len =
            u32::from_le_bytes(repl_data[..4].try_into().expect("4-byte length prefix")) as usize;
        assert_eq!(repl_data.len(), 4 + payload_len);
        let payload = &repl_data[4..];
        let slots: Vec<InputSlot> =
            melin_transport_core::replication_wire::try_decode_input_batch(payload)
                .expect("InputBatch decode");
        assert!(
            !slots.is_empty(),
            "InputBatch should carry at least one slot"
        );
        let first = &slots[0];
        assert_eq!(
            first.sequence, FIRST_SEQ,
            "first slot's sequence should match journal first user event"
        );
        assert!(matches!(
            first.event,
            JournalEvent::App(crate::trading_event::TradingEvent::SubmitOrder { .. })
        ));

        // Simulate replica acking — update the replication cursor.
        replication_cursor.store(repl_meta.end_sequence + 1, Ordering::Release);

        // Verify dual-cursor gating: both cursors advanced.
        let journal_pos = journal_cursor.get().load(Ordering::Acquire);
        let repl_pos = replication_cursor.load(Ordering::Acquire);
        let effective = journal_pos.min(repl_pos);
        assert!(
            effective > output.input_seq,
            "both cursors should have advanced"
        );

        shutdown.store(true, Ordering::Relaxed);
        let _writer = t_journal.join().unwrap();
        let _exchange = t_matching.join().unwrap();
    }

    #[test]
    fn replication_cursor_always_starts_at_max() {
        // Cursor should be u64::MAX regardless of replication mode.
        // When disabled: no replica, no gating.
        // When enabled: server works before a replica connects; cursor
        // only engages when the replica sends its first ack.
        let dir = tempfile::tempdir().unwrap();

        // Standalone mode.
        {
            let path = dir.path().join("standalone.journal");
            let exchange = Exchange::new();
            let writer = JournalWriter::create_default(&path).unwrap();
            let active_conns = Arc::new(AtomicU64::new(0));

            let out = build_pipeline_with_replication(
                exchange,
                writer,
                Duration::ZERO,
                active_conns,
                false,
                MAX_JOURNAL_BATCH,
                REPLICATION_RING_CAPACITY,
                false,
                false,
                false,
            );
            assert!(out.replication_consumers.is_none());
            assert_eq!(out.replication_cursor.load(Ordering::Relaxed), u64::MAX);
        }

        // Replication enabled — cursor still starts at u64::MAX.
        {
            let path = dir.path().join("repl_enabled.journal");
            let exchange = Exchange::new();
            let writer = JournalWriter::create_default(&path).unwrap();
            let active_conns = Arc::new(AtomicU64::new(0));

            let out = build_pipeline_with_replication(
                exchange,
                writer,
                Duration::ZERO,
                active_conns,
                true,
                MAX_JOURNAL_BATCH,
                REPLICATION_RING_CAPACITY,
                false,
                false,
                false,
            );
            assert!(out.replication_consumers.is_some());
            assert_eq!(
                out.replication_cursor.load(Ordering::Relaxed),
                u64::MAX,
                "replication cursor should start at MAX even when enabled"
            );
        }
    }

    /// Helper: build a minimal matching stage with a replicas_connected counter.
    /// Returns (input_producer, output_consumer, connected_counter, shutdown, join_handle).
    fn start_matching_with_halt(initial_connected: u32) -> MatchingHaltResult {
        let mut exchange = Exchange::new();
        exchange.add_instrument(InstrumentSpec {
            symbol: Symbol(1),
            base: CurrencyId(0),
            quote: CurrencyId(1),
        });
        exchange.deposit(AccountId(1), CurrencyId(1), 1_000_000);

        let (input_producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();
        let (output_producer, mut output_consumers) = ring::DisruptorBuilder::<OutputSlot>::new(64)
            .add_consumer()
            .build();
        let output_consumer = output_consumers.pop().unwrap();

        let dummy_cursor = Arc::new(Sequence::new(AtomicU64::new(0)));
        let events_counter = Arc::new(AtomicU64::new(0));
        let active_conns = Arc::new(AtomicU64::new(0));
        let counter = Arc::new(AtomicU32::new(initial_connected));

        let stage = MatchingStage::new(
            exchange,
            consumer,
            output_producer,
            events_counter,
            dummy_cursor,
            active_conns,
            Some(Arc::clone(&counter)),
            false,
        );

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || stage.run(&s));

        (input_producer, output_consumer, counter, shutdown, handle)
    }

    /// Consume outputs until we see the request terminator, returning
    /// all reports. The terminator is now `is_last_in_request=true`
    /// on the final slot — it may be a Report (when the event produced
    /// at least one) or a `BatchEnd`-payload slot (zero-report case).
    fn collect_reports(output: &mut ring::Consumer<OutputSlot>) -> Vec<ExecutionReport> {
        let mut reports = Vec::new();
        loop {
            if let Some((_, slot)) = output.try_consume() {
                if let OutputPayload::Report(r) = slot.payload {
                    reports.push(r);
                }
                if slot.is_last_in_request {
                    return reports;
                }
            }
            std::hint::spin_loop();
        }
    }

    #[test]
    fn halt_rejects_submit_order() {
        let (mut input, mut output, _flag, shutdown, handle) = start_matching_with_halt(0);

        input.publish(InputSlot {
            connection_id: 1,
            key_hash: 0xAA,
            request_seq: 1,
            sequence: 0,
            timestamp_ns: 0,
            event: JournalEvent::App(crate::trading_event::TradingEvent::SubmitOrder {
                symbol: Symbol(1),
                order: limit_order(100, AccountId(1), Side::Buy, 50, 10),
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        let reports = collect_reports(&mut output);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                order_id: OrderId(100),
                account: AccountId(1),
                reason: RejectReason::ReplicaDisconnected,
                ..
            }
        ));

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn halt_rejects_deposit() {
        let (mut input, mut output, _flag, shutdown, handle) = start_matching_with_halt(0);

        input.publish(InputSlot {
            connection_id: 1,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 0,
            event: JournalEvent::App(crate::trading_event::TradingEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 100,
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        let reports = collect_reports(&mut output);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::ReplicaDisconnected,
                ..
            }
        ));

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn halt_allows_query_stats() {
        let (mut input, mut output, _flag, shutdown, handle) = start_matching_with_halt(0);

        input.publish(InputSlot {
            connection_id: 1,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 0,
            event: JournalEvent::App(crate::trading_event::TradingEvent::QueryStats),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        // QueryStats always produces a single output slot — StatsHeader
        // carrying the request terminator (`is_last_in_request=true`).
        // The wire BatchEnd is emitted by the response stage from that
        // flag. Spin-poll without an iteration cap, matching
        // `collect_reports` and the other halt tests: under load the
        // matching thread can take longer than any fixed iteration
        // budget, but the response is guaranteed to arrive.
        let mut got_stats = false;
        loop {
            if let Some((_, slot)) = output.try_consume() {
                match slot.payload {
                    OutputPayload::QueryResponse(QueryResponse::Stats { .. }) => got_stats = true,
                    OutputPayload::Report(ExecutionReport::Rejected { reason, .. }) => {
                        panic!("QueryStats should not be rejected, got: {reason:?}");
                    }
                    _ => {}
                }
                if slot.is_last_in_request {
                    break;
                }
            }
            std::hint::spin_loop();
        }
        assert!(got_stats, "should have received StatsHeader");

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn halt_then_reconnect_resumes_trading() {
        let (mut input, mut output, flag, shutdown, handle) = start_matching_with_halt(0);

        // Submit while halted — rejected.
        input.publish(InputSlot {
            connection_id: 1,
            key_hash: 0xBB,
            request_seq: 1,
            sequence: 0,
            timestamp_ns: 0,
            event: JournalEvent::App(crate::trading_event::TradingEvent::SubmitOrder {
                symbol: Symbol(1),
                order: limit_order(200, AccountId(1), Side::Buy, 50, 10),
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        let reports = collect_reports(&mut output);
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::ReplicaDisconnected,
                ..
            }
        ));

        // Reconnect replica.
        flag.store(1, Ordering::Relaxed);

        // Retry the same seq — should succeed now (HWM was not advanced).
        input.publish(InputSlot {
            connection_id: 1,
            key_hash: 0xBB,
            request_seq: 1,
            sequence: 0,
            timestamp_ns: 0,
            event: JournalEvent::App(crate::trading_event::TradingEvent::SubmitOrder {
                symbol: Symbol(1),
                order: limit_order(200, AccountId(1), Side::Buy, 50, 10),
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        let reports = collect_reports(&mut output);
        assert!(
            reports
                .iter()
                .any(|r| matches!(r, ExecutionReport::Placed { .. })),
            "order should be placed after reconnect, got: {reports:?}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn standalone_mode_no_halt() {
        // replicas_connected = None → no halt check, events always processed.
        let mut exchange = Exchange::new();
        exchange.add_instrument(InstrumentSpec {
            symbol: Symbol(1),
            base: CurrencyId(0),
            quote: CurrencyId(1),
        });
        exchange.deposit(AccountId(1), CurrencyId(1), 1_000_000);

        let (mut input_producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();
        let (output_producer, mut output_consumers) = ring::DisruptorBuilder::<OutputSlot>::new(64)
            .add_consumer()
            .build();
        let mut output_consumer = output_consumers.pop().unwrap();

        let stage = MatchingStage::new(
            exchange,
            consumer,
            output_producer,
            Arc::new(AtomicU64::new(0)),
            Arc::new(Sequence::new(AtomicU64::new(0))),
            Arc::new(AtomicU64::new(0)),
            None, // standalone
            false,
        );

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || stage.run(&s));

        input_producer.publish(InputSlot {
            connection_id: 1,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 0,
            event: JournalEvent::App(crate::trading_event::TradingEvent::SubmitOrder {
                symbol: Symbol(1),
                order: limit_order(1, AccountId(1), Side::Buy, 50, 10),
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        let reports = collect_reports(&mut output_consumer);
        assert!(
            reports
                .iter()
                .any(|r| matches!(r, ExecutionReport::Placed { .. })),
            "standalone mode should process normally, got: {reports:?}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    /// Manual rotation: flipping `rotate_requested` causes the journal
    /// stage to archive the live segment at the next fsync boundary.
    /// Verifies (1) an archive at `.000001` is created, (2) the live
    /// journal continues taking new events, and (3) full recovery walks
    /// archive + live and reproduces the cumulative state.
    #[cfg(not(feature = "no-persist"))]
    #[test]
    fn journal_stage_rotates_on_manual_request() {
        use std::time::Duration as StdDuration;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rotate_manual.journal");
        let writer = JournalWriter::create_default(&path).unwrap();

        let (mut producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();

        let mut stage = JournalStage::new(
            writer,
            consumer,
            StdDuration::ZERO,
            MAX_JOURNAL_BATCH,
            false,
        );
        let rotate_flag = Arc::new(AtomicBool::new(false));
        stage.set_rotation(
            /* max_journal_bytes */ 0,
            Some(Arc::clone(&rotate_flag)),
        );

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || stage.run(&s));

        // Helper: publish a deposit event with a unique request_seq so
        // every event survives dedup at recovery time.
        let mut req_seq: u64 = 0;
        let mut publish_deposit = |amount: u64| {
            req_seq += 1;
            producer.publish(InputSlot {
                connection_id: 1,
                key_hash: 1,
                request_seq: req_seq,
                sequence: 0,
                timestamp_ns: 1_000_000_000 + req_seq,
                event: JournalEvent::App(crate::trading_event::TradingEvent::Deposit {
                    account: AccountId(1),
                    currency: CurrencyId(1),
                    amount,
                }),
                publish_ts: trace_ts(),
                recv_ts: trace_ts(),
            });
        };

        // Phase 1 — pre-rotation events.
        publish_deposit(100);
        publish_deposit(200);

        // Wait for both events to be fsynced into the live segment so
        // the archive captures them. Polled rather than fixed-sleep so
        // a slow CI machine doesn't intermittently rotate before phase-1
        // events have made it to disk.
        let archive_path = std::path::PathBuf::from(format!("{}.000001", path.display()));
        let pre_size_path = path.clone();
        let deadline = std::time::Instant::now() + StdDuration::from_secs(5);
        while std::time::Instant::now() < deadline
            && (!pre_size_path.exists()
                || std::fs::metadata(&pre_size_path)
                    .map(|m| m.len())
                    .unwrap_or(0)
                    < 4096)
        {
            std::thread::sleep(StdDuration::from_millis(20));
        }

        rotate_flag.store(true, Ordering::Release);

        // Drive a third event after the flag — fsyncing it forces the
        // journal stage past a `maybe_rotate` boundary.
        publish_deposit(50);

        // Wait for the archive to appear.
        let deadline = std::time::Instant::now() + StdDuration::from_secs(5);
        while !archive_path.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(StdDuration::from_millis(20));
        }
        assert!(
            archive_path.exists(),
            "archive {} should exist after manual rotation",
            archive_path.display()
        );

        // Phase 2 — events after the rotation must land in the live
        // (post-rotation) segment.
        publish_deposit(1000);

        // Allow the post-rotation event to fsync.
        std::thread::sleep(StdDuration::from_millis(150));

        shutdown.store(true, Ordering::Relaxed);
        let _writer = handle.join().unwrap();

        // Recovery via the multi-segment walker should produce a single
        // exchange with deposits totalling 100 + 200 + 50 + 1000 = 1350.
        let exchange = crate::journal::JournaledExchange::recover(&path).unwrap();
        let bal = exchange
            .exchange()
            .accounts()
            .balance(AccountId(1), CurrencyId(1))
            .available;
        assert_eq!(bal, 1350, "all deposits across the rotation must replay");
    }

    /// Size-threshold rotation: setting a small `max_journal_bytes`
    /// causes the stage to rotate without operator intervention. The
    /// threshold is engaged after the first batch crosses the limit.
    #[cfg(not(feature = "no-persist"))]
    #[test]
    fn journal_stage_rotates_on_size_threshold() {
        use std::time::Duration as StdDuration;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rotate_size.journal");
        let writer = JournalWriter::create_default(&path).unwrap();

        let (mut producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();

        let mut stage = JournalStage::new(
            writer,
            consumer,
            StdDuration::ZERO,
            MAX_JOURNAL_BATCH,
            false,
        );
        // Tiny threshold — any non-empty fsync will cross it. The
        // pre-allocation tail in the journal file means `valid_end`
        // grows in sector-size increments; using 1 byte ensures the
        // first batch fsync trips the trigger reliably.
        stage.set_rotation(/* max_journal_bytes */ 1, None);

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || stage.run(&s));

        producer.publish(InputSlot {
            connection_id: 1,
            key_hash: 1,
            request_seq: 1,
            sequence: 0,
            timestamp_ns: 1_000_000_000,
            event: JournalEvent::App(crate::trading_event::TradingEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 42,
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        let archive_path = std::path::PathBuf::from(format!("{}.000001", path.display()));
        let deadline = std::time::Instant::now() + StdDuration::from_secs(5);
        while !archive_path.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(StdDuration::from_millis(20));
        }

        shutdown.store(true, Ordering::Relaxed);
        let _ = handle.join().unwrap();

        assert!(
            archive_path.exists(),
            "size-threshold rotation should have produced {}",
            archive_path.display()
        );
    }

    /// ROTATE storm: many rapid sets of the flag between fsync
    /// boundaries collapse to a single rotation, not one rotation per
    /// store. Validates the `compare_exchange(true → false)` consume in
    /// `maybe_rotate`.
    ///
    /// The storm has to land between two fsyncs — that's the only
    /// window in which the journal stage can observe-and-collapse
    /// without rotating in between. The test sequences:
    ///   publish → fsync (no rotation flag) → storm 100× set →
    ///   publish → fsync (one rotation) → publish → fsync (no flag).
    /// Final archive count should be exactly 1.
    #[cfg(not(feature = "no-persist"))]
    #[test]
    fn rotate_storm_collapses_to_single_rotation() {
        use std::time::Duration as StdDuration;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("storm.journal");
        let writer = JournalWriter::create_default(&path).unwrap();

        let (mut producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();

        let mut stage = JournalStage::new(
            writer,
            consumer,
            StdDuration::ZERO,
            MAX_JOURNAL_BATCH,
            false,
        );
        let rotate_flag = Arc::new(AtomicBool::new(false));
        stage.set_rotation(
            /* max_journal_bytes */ 0,
            Some(Arc::clone(&rotate_flag)),
        );

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || stage.run(&s));

        let mut req_seq: u64 = 0;
        let mut publish = |amount: u64| {
            req_seq += 1;
            producer.publish(InputSlot {
                connection_id: 1,
                key_hash: 1,
                request_seq: req_seq,
                sequence: 0,
                timestamp_ns: 1_000_000 + req_seq,
                event: JournalEvent::App(crate::trading_event::TradingEvent::Deposit {
                    account: AccountId(1),
                    currency: CurrencyId(1),
                    amount,
                }),
                publish_ts: trace_ts(),
                recv_ts: trace_ts(),
            });
        };

        // Initial event so the stage has something to fsync against.
        publish(1);
        std::thread::sleep(StdDuration::from_millis(100));

        // Storm: 100 rapid sets. The journal stage is currently idle
        // (waiting for new events), so none of these stores can be
        // observed-and-cleared until the next fsync — which is what
        // we want.
        for _ in 0..100 {
            rotate_flag.store(true, Ordering::Release);
        }

        // Trigger an fsync. The journal stage observes the flag once,
        // CAS-clears it, rotates, and the remaining 99 stores were
        // collapsed onto the same rotation.
        publish(2);

        // Wait for the rotation + a follow-up fsync to confirm no
        // second rotation fires (the flag is already false).
        let archive_001 = std::path::PathBuf::from(format!("{}.000001", path.display()));
        let deadline = std::time::Instant::now() + StdDuration::from_secs(5);
        while !archive_001.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(StdDuration::from_millis(20));
        }
        publish(3);
        std::thread::sleep(StdDuration::from_millis(200));

        shutdown.store(true, Ordering::Relaxed);
        let _ = handle.join().unwrap();

        assert!(archive_001.exists(), ".000001 must exist");
        let archive_002 = std::path::PathBuf::from(format!("{}.000002", path.display()));
        assert!(
            !archive_002.exists(),
            "storm must collapse to a single rotation, but .000002 exists"
        );
    }

    /// Post-rotation events must land in the *live* segment, not in the
    /// just-archived one. This is a regression test for the io_uring
    /// fixed-file-registration bug: rotation closes the old live fd
    /// (now pointing at the archived inode) and opens a new one for
    /// the new live segment, but io_uring's `register_files` table
    /// still references the old fd. Without an explicit
    /// `register_files_update`, every subsequent SQE submitted with
    /// `types::Fixed(0)` writes into the archived inode rather than
    /// the new live file — overwriting pre-rotation events at the
    /// post-rotation write offset and silently losing data.
    ///
    /// To exercise the io_uring async-write path (rather than the
    /// sync partial-tail path that bypasses the registered fd), the
    /// test bursts enough deposits that each batch exceeds one
    /// sector and therefore goes through `take_batch_for_async_write`.
    ///
    /// The test reads each on-disk segment directly (rather than
    /// going through `JournaledExchange::recover`, which would mask
    /// the bug by summing balances across both segments) and asserts:
    ///   * the archive contains only pre-rotation sequences,
    ///   * the live segment contains only post-rotation sequences,
    ///   * no sequence appears in both.
    #[cfg(not(feature = "no-persist"))]
    #[test]
    fn post_rotation_events_land_in_live_not_archive() {
        use melin_journal::JournalReader;
        use std::time::Duration as StdDuration;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("post_rot.journal");
        let writer = JournalWriter::create_default(&path).unwrap();

        let (mut producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(1024)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();

        let mut stage = JournalStage::new(
            writer,
            consumer,
            StdDuration::ZERO,
            MAX_JOURNAL_BATCH,
            false,
        );
        let rotate_flag = Arc::new(AtomicBool::new(false));
        stage.set_rotation(
            /* max_journal_bytes */ 0,
            Some(Arc::clone(&rotate_flag)),
        );

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || stage.run(&s));

        let mut req_seq: u64 = 0;
        let mut publish = |producer: &mut ring::Producer<InputSlot>, amount: u64| {
            req_seq += 1;
            producer.publish(InputSlot {
                connection_id: 1,
                key_hash: 1,
                request_seq: req_seq,
                sequence: 0,
                timestamp_ns: 1_000_000 + req_seq,
                event: JournalEvent::App(crate::trading_event::TradingEvent::Deposit {
                    account: AccountId(1),
                    currency: CurrencyId(1),
                    amount,
                }),
                publish_ts: trace_ts(),
                recv_ts: trace_ts(),
            });
        };

        // Phase 1 (pre-rotation): enough events to cross one sector
        // (~50 deposits × ~50 bytes each ≈ 2.5 KB > one 512-byte sector
        // → io_uring async-write path engages, not the partial-tail
        // sync fallback that bypasses the registered fd).
        const PRE: u64 = 50;
        for i in 1..=PRE {
            publish(&mut producer, i);
        }

        // Set the rotate flag, then publish a tiny batch to drive the
        // fsync at which the flag is observed. Both phase-1 and this
        // marker batch end up in the archive; the rotation happens
        // *after* they are durable, when `maybe_rotate` fires at the
        // post-fsync boundary.
        rotate_flag.store(true, Ordering::Release);
        let archive_001 = std::path::PathBuf::from(format!("{}.000001", path.display()));
        publish(&mut producer, 9_999);
        let deadline = std::time::Instant::now() + StdDuration::from_secs(5);
        while !archive_001.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(StdDuration::from_millis(20));
        }
        assert!(archive_001.exists(), "archive must be created by rotation");

        // Phase 2 (post-rotation): a fresh burst submitted *after* the
        // archive file has appeared, so these events land in the new
        // live segment. This is the path that exercises the post-
        // rotation io_uring SQE — Fixed(0) must point to the new fd
        // for the writes to land in the live file rather than the
        // archived inode.
        const POST: u64 = 50;
        for i in 1..=POST {
            publish(&mut producer, 10_000 + i);
        }
        std::thread::sleep(StdDuration::from_millis(300));

        shutdown.store(true, Ordering::Relaxed);
        let _ = handle.join().unwrap();

        // Read each segment directly. Collect every entry's sequence,
        // skipping the GenesisHash anchors that segment recovery uses
        // for chain continuity.
        fn collect_app_seqs(p: &std::path::Path) -> Vec<u64> {
            let mut reader = JournalReader::<crate::trading_event::TradingEvent>::open(p).unwrap();
            let mut out = Vec::new();
            while let Some(entry) = reader.next_entry().unwrap() {
                if matches!(entry.event, JournalEvent::App(_)) {
                    out.push(entry.sequence);
                }
            }
            out
        }
        let archive_seqs = collect_app_seqs(&archive_001);
        let live_seqs = collect_app_seqs(&path);

        assert!(
            !archive_seqs.is_empty(),
            "archive must contain the pre-rotation events"
        );
        assert!(
            !live_seqs.is_empty(),
            "live segment must contain post-rotation events"
        );
        // Every archive seq must be < every live seq — i.e. the two
        // ranges must be cleanly disjoint and ordered.
        let archive_max = *archive_seqs.iter().max().unwrap();
        let live_min = *live_seqs.iter().min().unwrap();
        assert!(
            archive_max < live_min,
            "post-rotation events leaked into the archive: archive max={archive_max} \
             live min={live_min} archive_seqs={archive_seqs:?} live_seqs={live_seqs:?}"
        );
        // No overlap.
        let archive_set: std::collections::HashSet<u64> = archive_seqs.iter().copied().collect();
        for s in &live_seqs {
            assert!(
                !archive_set.contains(s),
                "seq {s} present in both archive and live — io_uring fd \
                 reregistration regression?"
            );
        }
    }
}
