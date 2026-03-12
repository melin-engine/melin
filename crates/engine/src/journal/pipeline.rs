//! Pipeline stages for the LMAX disruptor architecture.
//!
//! Two hot-path stages consume from an input disruptor in **parallel**:
//! 1. **Journal stage**: batch-writes events to the WAL, fsyncs once per batch.
//!    Advances its cursor only after fsync.
//! 2. **Matching stage**: executes commands on the `Exchange`, publishes responses
//!    to the output SPSC. Runs concurrently with the journal — no waiting for fsync.
//!
//! The **response stage** (in the server crate) consumes the output SPSC but
//! gates on the journal cursor before sending: a response is only sent to
//! the client after the corresponding event is durable on disk.
//!
//! This gives maximum pipeline parallelism (matching overlaps journal I/O)
//! while preserving persist-before-ack at the response boundary.

use std::sync::Arc;

use crate::exchange::Exchange;
use crate::journal::event::JournalEvent;
use crate::journal::trace::{TraceTimestamp, trace_ts};
use crate::journal::writer::JournalWriter;
use crate::types::ExecutionReport;

use trading_disruptor::padding::Sequence;
use trading_disruptor::ring;
use trading_disruptor::spsc;

/// Ring buffer capacity for the input disruptor (journal + matching consumers).
/// 2^20 = 1,048,576 slots. At ~72 bytes per slot, this is ~72 MiB — fits in
/// L3 cache on modern server CPUs. Provides ~100 ms of buffering at 10M
/// orders/sec, enough headroom for fsync stalls without backpressure.
pub const INPUT_RING_CAPACITY: usize = 1 << 20;

/// SPSC queue capacity for the output path (matching → response).
/// Matches the input ring size since one input event can produce multiple
/// output messages (e.g., Fill + BatchEnd).
pub const OUTPUT_RING_CAPACITY: usize = 1 << 20;

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
    /// Timestamp when the publisher wrote this slot to the disruptor.
    /// `()` (zero-sized) when `latency-trace` is disabled.
    pub publish_ts: TraceTimestamp,
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
            publish_ts: trace_ts(),
        }
    }
}

/// Slot in the output SPSC queue (matching → response).
///
/// Each slot carries either an execution report or a batch-end marker
/// for a specific connection, plus the input sequence it originated from
/// so the response stage can gate on journal completion.
#[derive(Debug, Clone, Copy)]
pub struct OutputSlot {
    /// Which client connection receives this response.
    pub connection_id: u64,
    /// Input disruptor sequence this output originated from.
    /// The response stage must not send this until the journal cursor
    /// has advanced past this value (i.e., the event is durable).
    pub input_seq: u64,
    /// The response payload.
    pub payload: OutputPayload,
    /// Timestamp when the matching stage finished processing this event.
    /// `()` (zero-sized) when `latency-trace` is disabled.
    pub match_complete_ts: TraceTimestamp,
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
            input_seq: 0,
            payload: OutputPayload::BatchEnd,
            match_complete_ts: trace_ts(),
        }
    }
}

/// Journal stage: consumes from the input disruptor, batch-writes events
/// to the WAL, and fsyncs once per batch.
///
/// Runs on a dedicated OS thread. Uses `read_batch` + `commit` so its
/// cursor only advances **after** fsync. The response stage reads this
/// cursor to know when events are durable.
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
    /// Uses `read_batch` + `commit` (not `consume_batch`) to ensure the
    /// journal cursor is only advanced **after** fsync. The response stage
    /// checks this cursor before sending — this is the persist-before-ack
    /// boundary.
    ///
    /// Returns the `JournalWriter` on shutdown for clean resource release.
    pub fn run(mut self, shutdown: &std::sync::atomic::AtomicBool) -> JournalWriter {
        let mut batch = [InputSlot::default(); MAX_JOURNAL_BATCH];

        #[cfg(feature = "latency-trace")]
        let mut wakeup_hist = crate::journal::trace::StageHistogram::new(
            "journal: disruptor wakeup (publish → journal consume)",
        );
        #[cfg(feature = "latency-trace")]
        let mut batch_hist =
            crate::journal::trace::StageHistogram::new("journal: batch processing (write + sync)");

        loop {
            if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                self.drain_remaining(&mut batch);
                #[cfg(feature = "latency-trace")]
                {
                    wakeup_hist.print_report();
                    batch_hist.print_report();
                }
                return self.writer;
            }

            // Read entries WITHOUT advancing the cursor.
            let count = self.consumer.read_batch(&mut batch, MAX_JOURNAL_BATCH);
            if count == 0 {
                std::hint::spin_loop();
                continue;
            }

            #[cfg(feature = "latency-trace")]
            let batch_start = trace_ts();

            #[cfg(feature = "latency-trace")]
            for slot in &batch[..count] {
                wakeup_hist.record_ns(crate::journal::trace::trace_elapsed_ns(
                    slot.publish_ts,
                    batch_start,
                ));
            }

            // Batch encode all events.
            for slot in &batch[..count] {
                if let Err(e) = self.writer.append_no_sync(&slot.event) {
                    eprintln!("journal encode error: {e}");
                }
            }

            // Single fsync for the entire batch.
            if let Err(e) = self.writer.sync() {
                eprintln!("journal sync error: {e}");
            }

            // NOW advance the cursor — the response stage uses this to know
            // events are safely on disk.
            self.consumer.commit(count);

            #[cfg(feature = "latency-trace")]
            batch_hist.record_ns(crate::journal::trace::trace_elapsed_ns(
                batch_start,
                trace_ts(),
            ));
        }
    }

    /// Drain any remaining entries from the ring buffer on shutdown.
    fn drain_remaining(&mut self, batch: &mut [InputSlot]) {
        loop {
            let count = self.consumer.read_batch(batch, MAX_JOURNAL_BATCH);
            if count == 0 {
                break;
            }
            for slot in &batch[..count] {
                let _ = self.writer.append_no_sync(&slot.event);
            }
            let _ = self.writer.sync();
            self.consumer.commit(count);
        }
    }
}

/// Matching stage: consumes from the input disruptor (in parallel with
/// the journal stage), executes commands on the Exchange, and publishes
/// responses to the output SPSC.
///
/// Runs on a dedicated OS thread. Does NOT wait for journal fsync —
/// the persist-before-ack check happens in the response stage.
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

        #[cfg(feature = "latency-trace")]
        let mut wakeup_hist = crate::journal::trace::StageHistogram::new(
            "matching: disruptor wakeup (publish → matching consume)",
        );
        #[cfg(feature = "latency-trace")]
        let mut execute_hist =
            crate::journal::trace::StageHistogram::new("matching: execute (process_event)");

        loop {
            if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                #[cfg(feature = "latency-trace")]
                {
                    wakeup_hist.print_report();
                    execute_hist.print_report();
                }
                return self.exchange;
            }

            let entry = self.consumer.try_consume();
            let Some((input_seq, slot)) = entry else {
                std::hint::spin_loop();
                continue;
            };

            #[cfg(feature = "latency-trace")]
            {
                let now = trace_ts();
                wakeup_hist.record_ns(crate::journal::trace::trace_elapsed_ns(
                    slot.publish_ts,
                    now,
                ));
            }

            reports.clear();

            #[cfg(feature = "latency-trace")]
            let exec_start = trace_ts();

            self.process_event(&slot, &mut reports);

            #[cfg(feature = "latency-trace")]
            let exec_end = trace_ts();

            #[cfg(feature = "latency-trace")]
            execute_hist.record_ns(crate::journal::trace::trace_elapsed_ns(
                exec_start, exec_end,
            ));

            #[allow(clippy::let_unit_value)] // ZST when latency-trace is disabled
            let match_complete_ts = trace_ts();

            // Publish execution reports to the output SPSC.
            // All output slots for this request carry the same input_seq
            // so the response stage can gate on journal completion.
            for report in &reports {
                self.output.publish(OutputSlot {
                    connection_id: slot.connection_id,
                    input_seq,
                    payload: OutputPayload::Report(*report),
                    match_complete_ts,
                });
            }

            // Signal end of batch for this request.
            self.output.publish(OutputSlot {
                connection_id: slot.connection_id,
                input_seq,
                payload: OutputPayload::BatchEnd,
                match_complete_ts,
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

/// Build the input disruptor and output SPSC, returning the stages and
/// the journal progress cursor for the response stage.
///
/// **Topology**: both journal and matching consumers are gated on the
/// producer (parallel). The matching stage does NOT wait for journal
/// fsync — the response stage gates on the journal cursor instead.
///
/// The caller (server) is responsible for building the response stage
/// and spawning all threads.
pub fn build_pipeline(
    exchange: Exchange,
    writer: JournalWriter,
) -> (
    ring::Producer<InputSlot>,
    JournalStage,
    MatchingStage,
    spsc::Consumer<OutputSlot>,
    Arc<Sequence>,
) {
    // Input disruptor: 1 producer, 2 parallel consumers.
    let (input_producer, mut consumers) =
        ring::DisruptorBuilder::<InputSlot>::new(INPUT_RING_CAPACITY)
            .add_consumer() // consumer 0: journal, gated on producer
            .add_consumer() // consumer 1: matching, gated on producer (parallel)
            .build();

    let matching_consumer = consumers.pop().expect("matching consumer");
    let journal_consumer = consumers.pop().expect("journal consumer");

    // Grab the journal's progress cursor before moving it into the stage.
    // The response stage will read this to gate on fsync completion.
    let journal_cursor = journal_consumer.progress_counter();

    // Output SPSC: matching → response.
    let (output_producer, output_consumer) = spsc::channel::<OutputSlot>(OUTPUT_RING_CAPACITY);

    let journal_stage = JournalStage::new(writer, journal_consumer);
    let matching_stage = MatchingStage::new(exchange, matching_consumer, output_producer);

    (
        input_producer,
        journal_stage,
        matching_stage,
        output_consumer,
        journal_cursor,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicBool, Ordering};

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

        let (mut producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();

        let consumer = consumers.pop().unwrap();
        let stage = JournalStage::new(writer, consumer);

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        producer.publish(InputSlot {
            connection_id: 1,
            event: JournalEvent::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(1),
                    base: CurrencyId(0),
                    quote: CurrencyId(1),
                },
            },
            publish_ts: trace_ts(),
        });
        producer.publish(InputSlot {
            connection_id: 1,
            event: JournalEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 100_000,
            },
            publish_ts: trace_ts(),
        });

        let handle = std::thread::spawn(move || stage.run(&shutdown2));

        std::thread::sleep(std::time::Duration::from_millis(50));
        shutdown.store(true, Ordering::Relaxed);
        let _writer = handle.join().unwrap();

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

        let (mut input_producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();

        let (output_producer, mut output_consumer) = spsc::channel::<OutputSlot>(64);

        let stage = MatchingStage::new(exchange, consumer, output_producer);

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        input_producer.publish(InputSlot {
            connection_id: 42,
            event: JournalEvent::SubmitOrder {
                symbol: Symbol(1),
                order: limit_order(1, AccountId(2), Side::Sell, 100, 50),
            },
            publish_ts: trace_ts(),
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

        let (
            mut input_producer,
            journal_stage,
            matching_stage,
            mut output_consumer,
            journal_cursor,
        ) = build_pipeline(exchange, writer);

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
            publish_ts: trace_ts(),
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

        // Verify the event was journaled.
        let mut reader = crate::journal::JournalReader::open(&path).unwrap();
        let entry = reader.next_entry().unwrap().unwrap();
        assert!(matches!(entry.event, JournalEvent::SubmitOrder { .. }));
    }
}
