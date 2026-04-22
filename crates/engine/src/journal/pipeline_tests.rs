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
    use crate::journal::pipeline::{
        MAX_JOURNAL_BATCH, build_pipeline_with_replication, build_replica_pipeline,
    };
    // Trading-bound concrete aliases for everything the tests
    // construct / pattern-match.
    use crate::exchange::Exchange;
    use crate::journal::replication::REPLICATION_RING_CAPACITY;
    use crate::journal::{
        InputSlot, JournalEvent, JournalStage, JournalWriter, MatchingStage, OutputPayload,
        OutputSlot,
    };
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

        let writer = JournalWriter::create(&path).unwrap();

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
        use crate::journal::CHECKPOINT_INTERVAL;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint_boundary.journal");
        let writer = JournalWriter::create(&path).unwrap();

        // Ring capacity: power-of-two large enough to hold every event
        // without the publisher ever blocking on the consumer. This lets
        // the pipeline exercise the full in-flight / auto-emit path.
        // Cross the checkpoint boundary at least twice so any off-by-one
        // around the auto-emit is exercised on both the first and second
        // segment.
        let total: u64 = CHECKPOINT_INTERVAL * 2 + 100;
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
        use crate::journal::CHECKPOINT_INTERVAL;
        use crate::journal::codec;

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
        let primary_writer =
            JournalWriter::create_continuing(&primary_path, 1, shared_genesis).unwrap();
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
        let replica_writer =
            JournalWriter::create_continuing(&replica_path, 1, shared_genesis).unwrap();
        let replica = build_replica_pipeline(
            replica_exchange,
            replica_writer,
            MAX_JOURNAL_BATCH,
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
        let replica_input = replica.input_producer.clone();

        let primary_shutdown = Arc::new(AtomicBool::new(false));
        let replica_shutdown = Arc::new(AtomicBool::new(false));
        let relay_shutdown = Arc::new(AtomicBool::new(false));

        // --- relay thread: pump primary's replication ring -> replica's input ring ---
        let relay_stop = Arc::clone(&relay_shutdown);
        let t_relay = std::thread::spawn(move || {
            loop {
                let mut got_something = false;
                // Ring 0: decode each batch's bytes into InputSlots with
                // the primary's sequence stamped, then publish to the
                // replica's input ring. Mirrors `submit_batch_to_pipeline`.
                if let Some((_meta, data)) = repl_c0.try_read() {
                    let mut off = 0;
                    while off < data.len() {
                        match codec::decode(&data[off..], codec::FORMAT_VERSION) {
                            Ok((
                                consumed,
                                sequence,
                                timestamp_ns,
                                key_hash,
                                request_seq,
                                event,
                            )) => {
                                off += consumed;
                                // Skip the primary's auto-emitted
                                // Checkpoint entries: the replica has a
                                // chain hash seeded from its own (test-
                                // local) genesis, so passing primary's
                                // Checkpoint through verify_primary_
                                // checkpoint would always diverge and
                                // kill the replica's JournalStage. The
                                // replica still auto-emits its own
                                // Checkpoints at the same sequence
                                // positions.
                                if matches!(event, JournalEvent::Checkpoint { .. }) {
                                    continue;
                                }
                                replica_input.publish(InputSlot {
                                    connection_id: 0,
                                    key_hash,
                                    request_seq,
                                    sequence,
                                    timestamp_ns,
                                    event,
                                    publish_ts: trace_ts(),
                                    recv_ts: trace_ts(),
                                });
                            }
                            Err(e) => panic!("relay decode failed at off={off}: {e}"),
                        }
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
        let total: u64 = CHECKPOINT_INTERVAL * 5 + 250;
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

        let writer = JournalWriter::create(&path).unwrap();

        let (mut producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();

        let consumer = consumers.pop().unwrap();
        let stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        // Publish events with pre-assigned sequences (simulating replica mode).
        // Start at sequence 2: when the hash-chain feature is enabled,
        // JournalWriter::create writes a GenesisHash at sequence 1, so the
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

        let writer = JournalWriter::create(&path).unwrap();

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

        let batch_end = loop {
            if let Some((_, slot)) = output_consumer.try_consume() {
                break slot;
            }
            std::hint::spin_loop();
        };
        assert!(matches!(batch_end.payload, OutputPayload::BatchEnd));

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

        let writer = JournalWriter::create(&path).unwrap();

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
        let input_producer = out.input_producer;
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

        let writer = JournalWriter::create(&path).unwrap();

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
        let input_producer = out.input_producer;
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

        // Verify the replication batch contains valid journal entries with
        // the same sequence numbers as the on-disk journal.
        let (consumed, seq, _ts, _kh, _rs, event) =
            melin_journal::codec::decode(&repl_data, melin_journal::codec::FORMAT_VERSION).unwrap();
        assert!(consumed > 0);
        assert_eq!(
            seq, FIRST_SEQ,
            "replication sequence should match journal first user event"
        );
        assert!(matches!(
            event,
            JournalEvent::App(crate::trading_event::TradingEvent::SubmitOrder { .. })
        ));

        // Verify the replicated bytes are byte-identical to what's on disk.
        #[cfg(not(feature = "no-persist"))]
        {
            use melin_journal::codec::FILE_HEADER_SIZE;
            let file_bytes = std::fs::read(&path).unwrap();

            // Find the start of user entries (after file header and genesis if present).
            let offset = {
                #[cfg(feature = "hash-chain")]
                {
                    // Skip past the genesis entry.
                    let genesis_len = u16::from_le_bytes([
                        file_bytes[FILE_HEADER_SIZE + 2],
                        file_bytes[FILE_HEADER_SIZE + 3],
                    ]) as usize;
                    FILE_HEADER_SIZE + 20 + genesis_len + 4
                }
                #[cfg(not(feature = "hash-chain"))]
                {
                    FILE_HEADER_SIZE
                }
            };

            // Find end of valid data via reader.
            let mut reader = crate::journal::JournalReader::open(&path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            let data_end = reader.valid_file_end() as usize;

            let disk_bytes = &file_bytes[offset..data_end];
            assert_eq!(
                repl_data, disk_bytes,
                "replicated bytes must be byte-identical to journal file"
            );
        }

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
            let writer = JournalWriter::create(&path).unwrap();
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
            let writer = JournalWriter::create(&path).unwrap();
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

    /// Consume outputs until we see a BatchEnd, returning all reports.
    fn collect_reports(output: &mut ring::Consumer<OutputSlot>) -> Vec<ExecutionReport> {
        let mut reports = Vec::new();
        loop {
            if let Some((_, slot)) = output.try_consume() {
                match slot.payload {
                    OutputPayload::Report(r) => reports.push(r),
                    OutputPayload::BatchEnd => return reports,
                    _ => {}
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

        // QueryStats produces StatsHeader + BatchEnd, not a Rejected.
        let mut got_stats = false;
        let mut got_batch_end = false;
        for _ in 0..1_000_000 {
            if let Some((_, slot)) = output.try_consume() {
                match slot.payload {
                    OutputPayload::QueryResponse(QueryResponse::Stats { .. }) => got_stats = true,
                    OutputPayload::BatchEnd => {
                        got_batch_end = true;
                        break;
                    }
                    OutputPayload::Report(ExecutionReport::Rejected { reason, .. }) => {
                        panic!("QueryStats should not be rejected, got: {reason:?}");
                    }
                    _ => {}
                }
            }
            std::hint::spin_loop();
        }
        assert!(got_stats, "should have received StatsHeader");
        assert!(got_batch_end, "should have received BatchEnd");

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
}
