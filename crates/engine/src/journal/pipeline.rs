//! Pipeline stages for the LMAX disruptor architecture.
//!
//! Two hot-path stages consume from an input disruptor (multi-consumer ring buffer):
//! 1. **Journal stage**: batch-writes events to the WAL, fsyncs once per batch.
//! 2. **Matching stage**: executes commands on the `Exchange`, publishes responses.
//!
//! The response stage (routing responses to per-connection channels) lives in the
//! server crate since it depends on tokio channels.

use crate::exchange::Exchange;
use crate::journal::event::JournalEvent;
use crate::journal::writer::JournalWriter;
use crate::types::ExecutionReport;

use trading_disruptor::ring;
use trading_disruptor::spsc;

/// Ring buffer capacity for the input disruptor (journal + matching consumers).
/// 2^16 = 65,536 slots — matches the server's command channel capacity.
pub const INPUT_RING_CAPACITY: usize = 1 << 16;

/// SPSC queue capacity for the output path (matching → response).
pub const OUTPUT_RING_CAPACITY: usize = 1 << 16;

/// Maximum number of events processed in one journal batch.
/// Limits the time spent encoding before fsync, keeping tail latency bounded.
const MAX_JOURNAL_BATCH: usize = 1024;

/// Slot in the input disruptor ring buffer.
///
/// Carries a connection ID alongside the event so the response stage knows
/// where to route execution reports. `Copy` for zero-cost ring buffer ops.
/// ~72 bytes: connection_id(8) + JournalEvent(~60) + padding.
#[derive(Debug, Clone, Copy)]
pub struct InputSlot {
    /// Which client connection submitted this command.
    pub connection_id: u64,
    /// The journaled event (order submit, cancel, etc.).
    pub event: JournalEvent,
}

impl Default for InputSlot {
    fn default() -> Self {
        // Default uses a zero-cost Deposit event as placeholder.
        // Ring buffer slots are always overwritten before being read,
        // so the default value is never observed.
        Self {
            connection_id: 0,
            event: JournalEvent::Deposit {
                account: crate::types::AccountId(0),
                currency: crate::types::CurrencyId(0),
                amount: 0,
            },
        }
    }
}

/// Slot in the output SPSC queue (matching → response).
///
/// Each slot carries either an execution report or a batch-end marker
/// for a specific connection. `Copy` for zero-cost ring buffer ops.
#[derive(Debug, Clone, Copy)]
pub struct OutputSlot {
    /// Which client connection receives this response.
    pub connection_id: u64,
    /// The response payload.
    pub payload: OutputPayload,
}

/// Payload within an output slot.
#[derive(Debug, Clone, Copy)]
pub enum OutputPayload {
    /// An execution report from matching.
    Report(ExecutionReport),
    /// Signals the end of reports for one request.
    BatchEnd,
    /// Internal error during matching.
    EngineError,
}

impl Default for OutputSlot {
    fn default() -> Self {
        Self {
            connection_id: 0,
            payload: OutputPayload::BatchEnd,
        }
    }
}

/// Journal stage: consumes from the input disruptor, batch-writes events
/// to the WAL, and fsyncs once per batch.
///
/// Runs on a dedicated OS thread. Advancing the journal consumer's sequence
/// signals to the matching stage that events are durable.
pub struct JournalStage {
    writer: JournalWriter,
    consumer: ring::Consumer<InputSlot>,
}

impl JournalStage {
    /// Create a new journal stage.
    pub fn new(writer: JournalWriter, consumer: ring::Consumer<InputSlot>) -> Self {
        Self { writer, consumer }
    }

    /// Run the journal stage loop. Blocks until shutdown is signaled.
    ///
    /// Returns the `JournalWriter` on shutdown for clean resource release.
    pub fn run(mut self, shutdown: &std::sync::atomic::AtomicBool) -> JournalWriter {
        let mut batch = [InputSlot::default(); MAX_JOURNAL_BATCH];

        loop {
            if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                self.drain_remaining(&mut batch);
                return self.writer;
            }

            let count = self.consumer.consume_batch(&mut batch, MAX_JOURNAL_BATCH);
            if count == 0 {
                std::hint::spin_loop();
                continue;
            }

            // Batch encode all events, then single fsync.
            for slot in &batch[..count] {
                if let Err(e) = self.writer.append_no_sync(&slot.event) {
                    eprintln!("journal encode error: {e}");
                }
            }

            // Single fsync for the entire batch — this is where the latency
            // amortization happens. Under load, one ~700µs fsync covers
            // potentially hundreds of events.
            if let Err(e) = self.writer.sync() {
                eprintln!("journal sync error: {e}");
            }
        }
    }

    /// Drain any remaining entries from the ring buffer on shutdown.
    fn drain_remaining(&mut self, batch: &mut [InputSlot]) {
        loop {
            let count = self.consumer.consume_batch(batch, MAX_JOURNAL_BATCH);
            if count == 0 {
                break;
            }
            for slot in &batch[..count] {
                let _ = self.writer.append_no_sync(&slot.event);
            }
            let _ = self.writer.sync();
        }
    }
}

/// Matching stage: consumes from the input disruptor (gated on journal),
/// executes commands on the Exchange, and publishes responses to the output SPSC.
///
/// Runs on a dedicated OS thread. Only processes events after the journal
/// stage has made them durable.
pub struct MatchingStage {
    exchange: Exchange,
    consumer: ring::Consumer<InputSlot>,
    output: spsc::Producer<OutputSlot>,
}

impl MatchingStage {
    /// Create a new matching stage.
    pub fn new(
        exchange: Exchange,
        consumer: ring::Consumer<InputSlot>,
        output: spsc::Producer<OutputSlot>,
    ) -> Self {
        Self {
            exchange,
            consumer,
            output,
        }
    }

    /// Run the matching stage loop. Blocks until shutdown.
    ///
    /// Returns the `Exchange` on shutdown for potential snapshot saving.
    pub fn run(mut self, shutdown: &std::sync::atomic::AtomicBool) -> Exchange {
        // Pre-allocated report buffer, reused across commands.
        let mut reports: Vec<ExecutionReport> = Vec::with_capacity(64);

        loop {
            if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                return self.exchange;
            }

            let entry = self.consumer.try_consume();
            let Some((_, slot)) = entry else {
                std::hint::spin_loop();
                continue;
            };

            reports.clear();
            self.process_event(&slot, &mut reports);

            // Publish execution reports to the output SPSC.
            for report in &reports {
                self.output.publish(OutputSlot {
                    connection_id: slot.connection_id,
                    payload: OutputPayload::Report(*report),
                });
            }

            // Signal end of batch for this request.
            self.output.publish(OutputSlot {
                connection_id: slot.connection_id,
                payload: OutputPayload::BatchEnd,
            });
        }
    }

    /// Execute a single event against the exchange.
    fn process_event(&mut self, slot: &InputSlot, reports: &mut Vec<ExecutionReport>) {
        match slot.event {
            JournalEvent::AddInstrument { spec } => {
                self.exchange.add_instrument(spec);
            }
            JournalEvent::Deposit {
                account,
                currency,
                amount,
            } => {
                self.exchange.deposit(account, currency, amount);
            }
            JournalEvent::SubmitOrder { symbol, order } => {
                self.exchange.execute(symbol, order, reports);
            }
            JournalEvent::CancelOrder { symbol, order_id } => {
                self.exchange.cancel(symbol, order_id, reports);
            }
        }
    }
}

/// Build the input disruptor and output SPSC, returning the stages and producers.
///
/// The caller (server) is responsible for building the response stage and
/// spawning all threads.
pub fn build_pipeline(
    exchange: Exchange,
    writer: JournalWriter,
) -> (
    ring::Producer<InputSlot>,
    JournalStage,
    MatchingStage,
    spsc::Consumer<OutputSlot>,
) {
    // Input disruptor: 1 producer, 2 consumers (journal → matching).
    let (input_producer, mut consumers) =
        ring::DisruptorBuilder::<InputSlot>::new(INPUT_RING_CAPACITY)
            .add_consumer() // consumer 0: journal, gated on producer
            .add_consumer_after(0) // consumer 1: matching, gated on journal
            .build();

    let matching_consumer = consumers.pop().expect("matching consumer");
    let journal_consumer = consumers.pop().expect("journal consumer");

    // Output SPSC: matching → response.
    let (output_producer, output_consumer) = spsc::channel::<OutputSlot>(OUTPUT_RING_CAPACITY);

    let journal_stage = JournalStage::new(writer, journal_consumer);
    let matching_stage = MatchingStage::new(exchange, matching_consumer, output_producer);

    (
        input_producer,
        journal_stage,
        matching_stage,
        output_consumer,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    fn limit_order(id: u64, account: AccountId, side: Side, price: u64, qty: u64) -> Order {
        Order {
            id: OrderId(id),
            account,
            side,
            order_type: OrderType::Limit {
                price: Price(NonZeroU64::new(price).unwrap()),
            },
            time_in_force: TimeInForce::GTC,
            quantity: Quantity(NonZeroU64::new(qty).unwrap()),
        }
    }

    #[test]
    fn journal_stage_batch_writes_and_syncs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pipeline_journal.journal");

        let writer = JournalWriter::create(&path).unwrap();

        // Create a minimal disruptor with one consumer (the journal stage).
        let (mut producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();

        let consumer = consumers.pop().unwrap();
        let stage = JournalStage::new(writer, consumer);

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        // Publish some events.
        producer.publish(InputSlot {
            connection_id: 1,
            event: JournalEvent::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(1),
                    base: CurrencyId(0),
                    quote: CurrencyId(1),
                },
            },
        });
        producer.publish(InputSlot {
            connection_id: 1,
            event: JournalEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 100_000,
            },
        });

        // Run journal stage briefly.
        let handle = std::thread::spawn(move || stage.run(&shutdown2));

        std::thread::sleep(std::time::Duration::from_millis(50));
        shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
        let _writer = handle.join().unwrap();

        // Verify events were journaled by reading them back.
        let mut reader = crate::journal::JournalReader::open(&path).unwrap();
        let entry1 = reader.next_entry().unwrap().unwrap();
        assert!(matches!(entry1.event, JournalEvent::AddInstrument { .. }));
        let entry2 = reader.next_entry().unwrap().unwrap();
        assert!(matches!(entry2.event, JournalEvent::Deposit { .. }));
        assert!(reader.next_entry().unwrap().is_none());
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

        // Create input disruptor (single consumer for matching).
        let (mut input_producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();

        // Create output SPSC.
        let (output_producer, mut output_consumer) = spsc::channel::<OutputSlot>(64);

        let stage = MatchingStage::new(exchange, consumer, output_producer);

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        // Submit a sell order.
        input_producer.publish(InputSlot {
            connection_id: 42,
            event: JournalEvent::SubmitOrder {
                symbol: Symbol(1),
                order: limit_order(1, AccountId(2), Side::Sell, 100, 50),
            },
        });

        let handle = std::thread::spawn(move || stage.run(&shutdown2));

        // Wait for output.
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
        assert!(matches!(
            output.payload,
            OutputPayload::Report(ExecutionReport::Placed { .. })
        ));

        // Consume the BatchEnd.
        let batch_end = loop {
            if let Some((_, slot)) = output_consumer.try_consume() {
                break slot;
            }
            std::hint::spin_loop();
        };
        assert!(matches!(batch_end.payload, OutputPayload::BatchEnd));

        shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
        let _exchange = handle.join().unwrap();
    }

    #[test]
    fn full_pipeline_journal_and_matching() {
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

        let (mut input_producer, journal_stage, matching_stage, mut output_consumer) =
            build_pipeline(exchange, writer);

        let shutdown = Arc::new(AtomicBool::new(false));
        let s1 = Arc::clone(&shutdown);
        let s2 = Arc::clone(&shutdown);

        let t_journal = std::thread::spawn(move || journal_stage.run(&s1));
        let t_matching = std::thread::spawn(move || matching_stage.run(&s2));

        // Submit an order through the pipeline.
        input_producer.publish(InputSlot {
            connection_id: 1,
            event: JournalEvent::SubmitOrder {
                symbol: Symbol(1),
                order: limit_order(1, AccountId(2), Side::Sell, 100, 50),
            },
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

        shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
        let _writer = t_journal.join().unwrap();
        let _exchange = t_matching.join().unwrap();

        // Verify the event was journaled.
        let mut reader = crate::journal::JournalReader::open(&path).unwrap();
        let entry = reader.next_entry().unwrap().unwrap();
        assert!(matches!(entry.event, JournalEvent::SubmitOrder { .. }));
    }
}
