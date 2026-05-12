//! JournaledExchange — wraps `Exchange` with durable event journaling.
//!
//! Journals every input command before executing it, ensuring the
//! persist-before-ack invariant. On crash, replay reconstructs identical state.

use std::path::Path;

use crate::exchange::Exchange;
use crate::types::{
    AccountId, CircuitBreakerConfig, CurrencyId, ExecutionReport, FeeSchedule, InstrumentSpec,
    Order, OrderId, RejectReason, RiskLimits, Symbol,
};

use crate::journal::JournalEvent;
use crate::journal::JournalReader;
#[cfg(test)]
use crate::journal::SectorWriter;
use crate::journal::snapshot;
use crate::trading_event::TradingEvent;
use melin_journal::{JournalError, JournalWrite};

/// Error surfaced by [`JournaledExchange::withdraw`]: either a journal
/// I/O failure or a business-level rejection (insufficient balance,
/// resting orders, unknown account). Kept engine-local so the
/// `melin-journal` crate stays free of trading types.
#[derive(Debug)]
pub enum JournaledExchangeError {
    /// Transport-level journal failure (I/O, CRC, version, …).
    Journal(JournalError),
    /// Exchange rejected the operation (business logic). The journal
    /// entry is still durable so replay reproduces this deterministically.
    Rejected(RejectReason),
}

impl std::fmt::Display for JournaledExchangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Journal(e) => write!(f, "journal error: {e}"),
            Self::Rejected(r) => write!(f, "command rejected: {r:?}"),
        }
    }
}

impl std::error::Error for JournaledExchangeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Journal(e) => Some(e),
            Self::Rejected(_) => None,
        }
    }
}

impl From<JournalError> for JournaledExchangeError {
    fn from(e: JournalError) -> Self {
        Self::Journal(e)
    }
}

/// Exchange wrapper that journals all input commands to a write-ahead log
/// before executing them. Provides crash recovery via journal replay.
///
/// Generic over `W` — the caller picks the concrete writer type
/// ([`crate::journal::BufferedWriter`] or [`crate::journal::SectorWriter`])
/// at construction time. The pipeline boot path dispatches on the
/// runtime mode flag and picks `W` once.
pub struct JournaledExchange<W: JournalWrite<TradingEvent>> {
    exchange: Exchange,
    writer: W,
}

impl<W: JournalWrite<TradingEvent>> JournaledExchange<W> {
    /// Create a new journaled exchange with a fresh journal file.
    pub fn create(journal_path: &Path) -> Result<Self, JournalError> {
        let writer = W::create(journal_path)?;
        Ok(Self {
            exchange: Exchange::with_capacity(),
            writer,
        })
    }

    /// Register a new instrument. Journals before executing.
    pub fn add_instrument(&mut self, spec: InstrumentSpec) -> Result<(), JournalError> {
        self.writer.append(&JournalEvent::App(
            crate::trading_event::TradingEvent::AddInstrument { spec },
        ))?;
        self.exchange.add_instrument(spec);
        Ok(())
    }

    /// Deposit funds. Journals before executing.
    pub fn deposit(
        &mut self,
        account: AccountId,
        currency: CurrencyId,
        amount: u64,
    ) -> Result<(), JournalError> {
        self.writer.append(&JournalEvent::App(
            crate::trading_event::TradingEvent::Deposit {
                account,
                currency,
                amount,
            },
        ))?;
        self.exchange.deposit(account, currency, amount);
        Ok(())
    }

    /// Cancel all orders for an account (kill switch). Journals before executing.
    pub fn cancel_all(
        &mut self,
        account: AccountId,
        reports: &mut Vec<ExecutionReport>,
    ) -> Result<(), JournalError> {
        self.writer.append(&JournalEvent::App(
            crate::trading_event::TradingEvent::CancelAll { account },
        ))?;
        self.exchange.cancel_all(account, reports);
        Ok(())
    }

    /// Withdraw funds from an account. Journals before executing.
    /// Rejects if the account has resting orders or insufficient balance.
    ///
    /// The journal entry is appended unconditionally so that replay
    /// reproduces the same rejection deterministically. Business-level
    /// rejections (insufficient balance, unknown account, resting orders)
    /// are surfaced to the caller as `JournaledExchangeError::Rejected`.
    pub fn withdraw(
        &mut self,
        account: AccountId,
        currency: CurrencyId,
        amount: u64,
    ) -> Result<(), JournaledExchangeError> {
        self.writer.append(&JournalEvent::App(
            crate::trading_event::TradingEvent::Withdraw {
                account,
                currency,
                amount,
            },
        ))?;
        self.exchange
            .withdraw(account, currency, amount)
            .map_err(JournaledExchangeError::Rejected)
    }

    /// Set risk limits for an instrument. Journals before executing.
    pub fn set_risk_limits(
        &mut self,
        symbol: Symbol,
        limits: RiskLimits,
    ) -> Result<(), JournalError> {
        self.writer.append(&JournalEvent::App(
            crate::trading_event::TradingEvent::SetRiskLimits { symbol, limits },
        ))?;
        self.exchange.set_risk_limits(symbol, limits);
        Ok(())
    }

    /// Set circuit breaker configuration for an instrument. Journals before executing.
    pub fn set_circuit_breaker(
        &mut self,
        symbol: Symbol,
        config: CircuitBreakerConfig,
    ) -> Result<(), JournalError> {
        self.writer.append(&JournalEvent::App(
            crate::trading_event::TradingEvent::SetCircuitBreaker { symbol, config },
        ))?;
        self.exchange.set_circuit_breaker(symbol, config);
        Ok(())
    }

    /// Set the fee schedule for an instrument. Journals before executing.
    /// May cancel orders whose accounts can't afford the new fee cushion.
    pub fn set_fee_schedule(
        &mut self,
        symbol: Symbol,
        schedule: FeeSchedule,
        reports: &mut Vec<ExecutionReport>,
    ) -> Result<(), JournalError> {
        self.writer.append(&JournalEvent::App(
            crate::trading_event::TradingEvent::SetFeeSchedule { symbol, schedule },
        ))?;
        self.exchange.set_fee_schedule(symbol, schedule, reports);
        Ok(())
    }

    /// Submit an order. Journals before executing.
    pub fn execute(
        &mut self,
        symbol: Symbol,
        order: Order,
        reports: &mut Vec<ExecutionReport>,
    ) -> Result<(), JournalError> {
        self.writer.append(&JournalEvent::App(
            crate::trading_event::TradingEvent::SubmitOrder { symbol, order },
        ))?;
        self.exchange.execute(symbol, order, reports);
        Ok(())
    }

    /// Cancel an order. Journals before executing.
    pub fn cancel(
        &mut self,
        symbol: Symbol,
        account: AccountId,
        order_id: OrderId,
        reports: &mut Vec<ExecutionReport>,
    ) -> Result<(), JournalError> {
        self.writer.append(&JournalEvent::App(
            crate::trading_event::TradingEvent::CancelOrder {
                symbol,
                account,
                order_id,
            },
        ))?;
        self.exchange.cancel(symbol, account, order_id, reports);
        Ok(())
    }

    /// Disable an instrument. Journals before executing.
    pub fn disable_instrument(
        &mut self,
        symbol: Symbol,
        reports: &mut Vec<ExecutionReport>,
    ) -> Result<(), JournalError> {
        self.writer.append(&JournalEvent::App(
            crate::trading_event::TradingEvent::DisableInstrument { symbol },
        ))?;
        self.exchange.disable_instrument(symbol, reports);
        Ok(())
    }

    /// Re-enable a disabled instrument. Journals before executing.
    pub fn enable_instrument(
        &mut self,
        symbol: Symbol,
        reports: &mut Vec<ExecutionReport>,
    ) -> Result<(), JournalError> {
        self.writer.append(&JournalEvent::App(
            crate::trading_event::TradingEvent::EnableInstrument { symbol },
        ))?;
        self.exchange.enable_instrument(symbol, reports);
        Ok(())
    }

    /// Remove a disabled instrument. Journals before executing.
    pub fn remove_instrument(
        &mut self,
        symbol: Symbol,
        reports: &mut Vec<ExecutionReport>,
    ) -> Result<(), JournalError> {
        self.writer.append(&JournalEvent::App(
            crate::trading_event::TradingEvent::RemoveInstrument { symbol },
        ))?;
        self.exchange.remove_instrument(symbol, reports);
        Ok(())
    }

    /// Advance the engine clock to `now_ns`, draining any due scheduled
    /// tasks. Journals the tick first so replay reproduces the same firing
    /// boundary. In production the tick is published by the dedicated tick
    /// thread onto the pipeline ring; this entry point exists for tests and
    /// for callers that drive a JournaledExchange directly.
    pub fn tick(
        &mut self,
        now_ns: u64,
        reports: &mut Vec<ExecutionReport>,
    ) -> Result<(), JournalError> {
        self.writer.append(&JournalEvent::Tick { now_ns })?;
        self.exchange.drain_due_scheduled_tasks(now_ns, reports);
        Ok(())
    }

    /// Recover from an existing journal file by replaying all events.
    ///
    /// Truncates any trailing garbage from a partial write (crash recovery),
    /// then reopens the writer for appending new events.
    pub fn recover(journal_path: &Path) -> Result<Self, JournalError> {
        Self::recover_inner(journal_path, None)
    }

    /// Recover from a snapshot plus a journal file.
    pub fn recover_from_snapshot(
        snapshot_path: &Path,
        journal_path: &Path,
    ) -> Result<Self, JournalError> {
        let (exchange, snap_sequence, snap_chain_hash) = snapshot::load(snapshot_path)?;
        Self::recover_inner_with_state(
            exchange,
            journal_path,
            Some((snap_sequence, snap_chain_hash)),
        )
    }

    /// Multi-segment recovery driver. Walks every archived segment in
    /// order, then the live segment, replaying app events on top of a
    /// fresh `Exchange`. See [`Self::recover_inner_with_state`] for the
    /// snapshot-aware path that mirrors this for the snapshot case.
    fn recover_inner(
        journal_path: &Path,
        snapshot: Option<(u64, [u8; 32])>,
    ) -> Result<Self, JournalError> {
        Self::recover_inner_with_state(Exchange::with_capacity(), journal_path, snapshot)
    }

    fn recover_inner_with_state(
        mut exchange: Exchange,
        journal_path: &Path,
        snapshot: Option<(u64, [u8; 32])>,
    ) -> Result<Self, JournalError> {
        let archives =
            melin_journal::segment::list_archives(journal_path).map_err(JournalError::Io)?;
        let snap_sequence = snapshot.map(|(s, _)| s).unwrap_or(0);
        let mut reports = Vec::new();
        let mut last_drain_ns: u64 = 0;
        let mut prev_tail_hash: Option<[u8; 32]> = None;
        // Highest sequence observed across walked archives. Used to
        // synthesize a new live when rotation crashed mid-rename — see
        // [`JournaledApp::recover_inner`] for the full Phase B
        // rationale.
        let mut last_seq_seen: u64 = snap_sequence;

        for (idx, archive_path) in &archives {
            let mut reader = JournalReader::open(archive_path)?;
            replay_segment_into_exchange(
                &mut reader,
                &mut exchange,
                snap_sequence,
                &mut last_drain_ns,
                &mut reports,
                /* allow_partial_tail = */ false,
            )?;
            verify_segment_boundary(*idx, prev_tail_hash, reader.genesis_payload())?;
            if let Some(h) = reader.chain_hash() {
                prev_tail_hash = Some(h);
            }
            if let Some(seq) = reader.last_sequence() {
                last_seq_seen = last_seq_seen.max(seq);
            }
        }

        if !journal_path.exists() {
            // Phase B: live missing but archives present — rotation
            // crashed between the rename and the new live's creation.
            // Synthesize a fresh live segment continuing from the last
            // archive's tail so the pipeline has somewhere to append.
            if archives.is_empty() {
                return Err(JournalError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "no live journal and no archives — nothing to recover",
                )));
            }
            let genesis = prev_tail_hash.unwrap_or([0u8; 32]);
            let writer = W::create_continuing(journal_path, last_seq_seen + 1, genesis)?;
            return Ok(Self { exchange, writer });
        }

        let mut reader = JournalReader::open(journal_path)?;
        replay_segment_into_exchange(
            &mut reader,
            &mut exchange,
            snap_sequence,
            &mut last_drain_ns,
            &mut reports,
            /* allow_partial_tail = */ true,
        )?;
        verify_segment_boundary(0, prev_tail_hash, reader.genesis_payload())?;

        let last_seq = reader.last_sequence().unwrap_or(snap_sequence);
        let valid_end = reader.valid_file_end();
        let chain_hash = reader.chain_hash();
        let events_since_checkpoint = reader.events_since_checkpoint();
        tracing::debug!(last_seq, valid_end, "recover: opening append");
        let writer = W::open_append(
            journal_path,
            last_seq,
            valid_end,
            chain_hash,
            events_since_checkpoint,
        )?;

        Ok(Self { exchange, writer })
    }

    /// Save a snapshot of the current exchange state.
    ///
    /// The snapshot records the current journal sequence and chain hash
    /// so recovery knows where to start replaying and can resume the
    /// hash chain without replaying from genesis.
    pub fn save_snapshot(&self, snapshot_path: &Path) -> Result<(), JournalError> {
        // Snapshot captures state as of the last journaled event.
        let seq = self.writer.next_sequence().saturating_sub(1);
        let chain_hash = self.writer.chain_hash().unwrap_or([0u8; 32]);
        snapshot::save(&self.exchange, seq, chain_hash, snapshot_path)
    }

    /// Access the underlying exchange (for queries like balance checks).
    pub fn exchange(&self) -> &Exchange {
        &self.exchange
    }

    /// Current journal sequence number (next to be assigned).
    pub fn next_sequence(&self) -> u64 {
        self.writer.next_sequence()
    }

    /// Path to the journal file.
    pub fn journal_path(&self) -> &Path {
        self.writer.path()
    }

    /// Current BLAKE3 chain hash (for testing and diagnostics).
    pub fn writer_chain_hash(&self) -> Option<[u8; 32]> {
        self.writer.chain_hash()
    }

    /// Rotate the journal: save a snapshot, archive the old journal, and
    /// start writing to a new empty journal file.
    ///
    /// The new journal continues the sequence numbering from the old one,
    /// so recovery from the snapshot + new journal produces the same state.
    ///
    /// The old journal is renamed to its next monotonic archive slot
    /// (`<path>.NNNNNN`). The snapshot is written to `snapshot_path`
    /// atomically (via `.tmp` + rename).
    ///
    /// Call this before `into_parts()` — rotation requires both the
    /// exchange (for snapshot) and the writer (for sequence continuity).
    pub fn rotate(&mut self, snapshot_path: &Path) -> Result<(), JournalError> {
        self.save_snapshot(snapshot_path)?;
        // The writer's `rotate_segment` archives the live file and opens
        // a fresh continuing live in one step — sequence and chain
        // continuity are both writer-internal concerns.
        self.writer.rotate_segment()?;
        Ok(())
    }

    /// Size of the current journal file in bytes.
    pub fn journal_size(&self) -> u64 {
        self.writer.valid_end()
    }

    /// Construct from pre-built parts. Used by the server for snapshot-only
    /// recovery (when the journal is missing after a rotation crash).
    pub fn from_parts(exchange: Exchange, writer: W) -> Self {
        Self { exchange, writer }
    }

    /// Decompose into parts for the pipeline architecture.
    ///
    /// After recovery, the exchange and journal writer are handed to separate
    /// pipeline stages: the matching thread owns the `Exchange`, and the
    /// journal thread owns the writer.
    pub fn into_parts(self) -> (Exchange, W) {
        (self.exchange, self.writer)
    }
}

/// Replay a single journal event into an exchange. Used during recovery.
///
/// Rebuilds the per-key request sequence HWM by calling `check_request_seq`
/// on every event. Since the journal contains no duplicates (they were
/// rejected at write time), this always returns true — the purpose is
/// to reconstruct the HWM state for live dedup after recovery.
///
/// Replay a single segment into an `Exchange`, skipping events whose
/// sequence is `<= snap_sequence`. `allow_partial_tail` controls how
/// `SequenceGap` is handled — an archived segment is sealed (any gap is
/// corruption), while the live segment may have a torn tail from a crash.
fn replay_segment_into_exchange(
    reader: &mut JournalReader,
    exchange: &mut Exchange,
    snap_sequence: u64,
    last_drain_ns: &mut u64,
    reports: &mut Vec<ExecutionReport>,
    allow_partial_tail: bool,
) -> Result<(), JournalError> {
    loop {
        match reader.next_entry() {
            Ok(Some(entry)) => {
                if entry.sequence > snap_sequence {
                    replay_event(
                        exchange,
                        &entry.event,
                        entry.timestamp_ns,
                        entry.key_hash,
                        entry.request_seq,
                        last_drain_ns,
                        reports,
                    );
                    reports.clear();
                }
            }
            Ok(None) => break,
            Err(JournalError::SequenceGap { expected, actual }) => {
                if allow_partial_tail {
                    tracing::warn!(
                        expected,
                        actual,
                        "sequence gap during recovery — truncating at gap"
                    );
                    break;
                }
                return Err(JournalError::SequenceGap { expected, actual });
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Verify that this segment's `GenesisHash` payload equals the previous
/// segment's tail chain hash. `index = 0` denotes the live segment.
/// No-op when either side is `None`.
fn verify_segment_boundary(
    index: u32,
    prev_tail: Option<[u8; 32]>,
    genesis_payload: Option<[u8; 32]>,
) -> Result<(), JournalError> {
    if let (Some(expected), Some(actual)) = (prev_tail, genesis_payload)
        && expected != actual
    {
        return Err(JournalError::SegmentChainBreak {
            index,
            expected,
            actual,
        });
    }
    Ok(())
}

/// `timestamp_ns` is the journaled wall-clock stamp from this entry; it
/// drives the same per-event scheduler drain that the live matching stage
/// performs, keeping replay state byte-identical to the live system.
/// `last_drain_ns` is caller-tracked across the replay loop so the drain
/// stays monotonic.
fn replay_event(
    exchange: &mut Exchange,
    event: &JournalEvent,
    timestamp_ns: u64,
    key_hash: u64,
    request_seq: u64,
    last_drain_ns: &mut u64,
    reports: &mut Vec<ExecutionReport>,
) {
    // Rebuild per-key HWM state (always succeeds on journal replay — no
    // duplicates in the journal).
    exchange.check_request_seq(key_hash, request_seq);

    // Mirror the matching stage's per-event scheduler drain — without this,
    // replay would not fire scheduled tasks at the same points the live
    // system did, and the recovered Exchange would diverge.
    if timestamp_ns > *last_drain_ns {
        *last_drain_ns = timestamp_ns;
        exchange.drain_due_scheduled_tasks(timestamp_ns, reports);
    }

    match *event {
        JournalEvent::App(crate::trading_event::TradingEvent::AddInstrument { spec }) => {
            exchange.add_instrument(spec);
        }
        JournalEvent::App(crate::trading_event::TradingEvent::Deposit {
            account,
            currency,
            amount,
        }) => {
            exchange.deposit(account, currency, amount);
        }
        JournalEvent::App(crate::trading_event::TradingEvent::SubmitOrder { symbol, order }) => {
            exchange.execute(symbol, order, reports);
        }
        JournalEvent::App(crate::trading_event::TradingEvent::CancelOrder {
            symbol,
            account,
            order_id,
        }) => {
            exchange.cancel(symbol, account, order_id, reports);
        }
        JournalEvent::App(crate::trading_event::TradingEvent::SetRiskLimits { symbol, limits }) => {
            exchange.set_risk_limits(symbol, limits);
        }
        JournalEvent::App(crate::trading_event::TradingEvent::CancelAll { account }) => {
            exchange.cancel_all(account, reports);
        }
        JournalEvent::App(crate::trading_event::TradingEvent::EndOfDay) => {
            exchange.end_of_day(reports);
        }
        JournalEvent::App(crate::trading_event::TradingEvent::SetCircuitBreaker {
            symbol,
            config,
        }) => {
            exchange.set_circuit_breaker(symbol, config);
        }
        JournalEvent::App(crate::trading_event::TradingEvent::CancelReplace {
            symbol,
            account,
            order_id,
            new_price,
            new_quantity,
        }) => {
            exchange.cancel_replace(symbol, account, order_id, new_price, new_quantity, reports);
        }
        JournalEvent::App(crate::trading_event::TradingEvent::SetFeeSchedule {
            symbol,
            schedule,
        }) => {
            exchange.set_fee_schedule(symbol, schedule, reports);
        }
        JournalEvent::App(crate::trading_event::TradingEvent::ProvisionAccount {
            account,
            amount,
        }) => {
            exchange.provision_account(account, amount);
        }
        JournalEvent::App(crate::trading_event::TradingEvent::Withdraw {
            account,
            currency,
            amount,
        }) => {
            // Withdraw errors (insufficient balance, resting orders) are
            // non-fatal on replay — the journal recorded the attempt, and
            // the original error was already returned to the client.
            let _ = exchange.withdraw(account, currency, amount);
        }
        JournalEvent::App(crate::trading_event::TradingEvent::DisableInstrument { symbol }) => {
            exchange.disable_instrument(symbol, reports);
        }
        JournalEvent::App(crate::trading_event::TradingEvent::EnableInstrument { symbol }) => {
            exchange.enable_instrument(symbol, reports);
        }
        JournalEvent::App(crate::trading_event::TradingEvent::RemoveInstrument { symbol }) => {
            exchange.remove_instrument(symbol, reports);
        }
        JournalEvent::Tick { now_ns } => {
            // Defensive: head-of-event drain typically already advanced to
            // this point. Re-draining via the explicit `now_ns` payload
            // keeps the contract consistent for callers that pass
            // `timestamp_ns = 0` (tests, manually replayed entries).
            exchange.drain_due_scheduled_tasks(now_ns, reports);
        }
        JournalEvent::App(crate::trading_event::TradingEvent::QueryStats)
        | JournalEvent::App(crate::trading_event::TradingEvent::QueryPosition { .. })
        | JournalEvent::App(crate::trading_event::TradingEvent::QueryRequestSeq) => {
            // Read-only queries are never journaled, so they should never
            // appear during replay. No-op if they somehow do.
        }
        JournalEvent::GenesisHash { .. } | JournalEvent::Checkpoint { .. } => {
            // Hash chain metadata — no exchange state change.
        }
        JournalEvent::Shutdown => {
            // Pipeline-only sentinel — never journaled, so unreachable on
            // the replay path. Defensive no-op.
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::journal::BufferedWriter;
    use crate::types::*;

    // Concrete writer used by every test in this module. The choice
    // doesn't affect the behaviour under test — all journaling paths
    // produce the same on-disk layout — so we standardise on the
    // production default (buffered + fdatasync) for portability.
    type TestExchange = JournaledExchange<BufferedWriter>;

    /// First user-event sequence: 2 with hash-chain (genesis takes 1), 1 without.
    #[cfg(feature = "hash-chain")]
    const FIRST_SEQ: u64 = 2;
    #[cfg(not(feature = "hash-chain"))]
    const FIRST_SEQ: u64 = 1;

    const ACCT_A: AccountId = AccountId(1);
    const ACCT_B: AccountId = AccountId(2);
    const BTC: CurrencyId = CurrencyId(0);
    const USD: CurrencyId = CurrencyId(1);

    fn btc_usd_spec() -> InstrumentSpec {
        InstrumentSpec {
            symbol: Symbol(1),
            base: BTC,
            quote: USD,
        }
    }

    fn qty(n: u64) -> Quantity {
        Quantity(NonZeroU64::new(n).unwrap())
    }

    fn price(n: u64) -> Price {
        Price(NonZeroU64::new(n).unwrap())
    }

    fn limit_order(id: u64, account: AccountId, side: Side, p: u64, q: u64) -> Order {
        Order {
            id: OrderId(id),
            account,
            side,
            order_type: OrderType::Limit {
                price: price(p),
                post_only: false,
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(q),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        }
    }

    #[test]
    fn replay_reproduces_identical_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replay.journal");

        // Build up some state. Genesis consumes seq 1; user events start at 2.
        let mut reports = Vec::new();
        {
            let mut je = TestExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
            je.deposit(ACCT_B, BTC, 500).unwrap();

            // Seller places ask.
            je.execute(
                Symbol(1),
                limit_order(1, ACCT_B, Side::Sell, 100, 50),
                &mut reports,
            )
            .unwrap();
            reports.clear();

            // Buyer places bid, fills against seller.
            je.execute(
                Symbol(1),
                limit_order(2, ACCT_A, Side::Buy, 100, 30),
                &mut reports,
            )
            .unwrap();
            reports.clear();

            // Another resting buy.
            je.execute(
                Symbol(1),
                limit_order(3, ACCT_A, Side::Buy, 95, 20),
                &mut reports,
            )
            .unwrap();
            reports.clear();

            // Cancel the resting buy.
            je.cancel(Symbol(1), ACCT_A, OrderId(3), &mut reports)
                .unwrap();
            reports.clear();

            // Verify state.
            assert_eq!(
                je.exchange().accounts().balance(ACCT_A, USD).available,
                97_000 // 100_000 - 3000 (fill 30@100)
            );
            assert_eq!(je.exchange().accounts().balance(ACCT_A, BTC).available, 30);
            assert_eq!(
                je.exchange().accounts().balance(ACCT_B, USD).available,
                3_000
            );
            assert_eq!(je.exchange().accounts().balance(ACCT_B, BTC).available, 450);
        }

        // Recover and verify identical state.
        let recovered = TestExchange::recover(&path).unwrap();
        assert_eq!(
            recovered
                .exchange()
                .accounts()
                .balance(ACCT_A, USD)
                .available,
            97_000
        );
        assert_eq!(
            recovered
                .exchange()
                .accounts()
                .balance(ACCT_A, BTC)
                .available,
            30
        );
        assert_eq!(
            recovered
                .exchange()
                .accounts()
                .balance(ACCT_B, USD)
                .available,
            3_000
        );
        assert_eq!(
            recovered
                .exchange()
                .accounts()
                .balance(ACCT_B, BTC)
                .available,
            450
        );
    }

    #[test]
    fn replay_produces_identical_reports() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reports.journal");

        let mut original_reports = Vec::new();
        {
            let mut je = TestExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
            je.deposit(ACCT_B, BTC, 500).unwrap();

            je.execute(
                Symbol(1),
                limit_order(1, ACCT_B, Side::Sell, 100, 50),
                &mut original_reports,
            )
            .unwrap();
            je.execute(
                Symbol(1),
                limit_order(2, ACCT_A, Side::Buy, 100, 30),
                &mut original_reports,
            )
            .unwrap();
        }

        // Replay should produce identical reports.
        let mut reader = crate::journal::JournalReader::open(&path).unwrap();
        let mut replay_exchange = Exchange::new();
        let mut replay_reports = Vec::new();
        let mut last_drain_ns: u64 = 0;

        while let Some(entry) = reader.next_entry().unwrap() {
            replay_event(
                &mut replay_exchange,
                &entry.event,
                entry.timestamp_ns,
                entry.key_hash,
                entry.request_seq,
                &mut last_drain_ns,
                &mut replay_reports,
            );
        }

        assert_eq!(original_reports, replay_reports);
    }

    #[test]
    fn ticks_drive_gtd_expiry_through_replay() {
        use crate::journal::wall_clock_nanos;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gtd_ticks.journal");

        // The matching/replay stage drains the scheduler at every event
        // using the journal entry's `timestamp_ns` (wall-clock at write
        // time). GTD `expiry_ns` therefore has to be in the same wall-clock
        // domain, comfortably in the future of every entry's timestamp,
        // for the test to control which Tick fires which order.
        let now = wall_clock_nanos();
        let expiry_one = now + 60 * 1_000_000_000; // +60s
        let expiry_two = now + 120 * 1_000_000_000; // +120s

        let mut reports = Vec::new();
        {
            let mut je = TestExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 1_000_000).unwrap();

            for (id, expiry) in [(1u64, expiry_one), (2, expiry_two)] {
                je.execute(
                    Symbol(1),
                    Order {
                        id: OrderId(id),
                        account: ACCT_A,
                        side: Side::Buy,
                        order_type: OrderType::Limit {
                            price: price(100 + id),
                            post_only: false,
                        },
                        time_in_force: TimeInForce::GTD,
                        quantity: qty(1),
                        stp: SelfTradeProtection::Allow,
                        expiry_ns: expiry,
                    },
                    &mut reports,
                )
                .unwrap();
            }
            reports.clear();

            // Tick past order 1's expiry but not order 2's.
            je.tick(expiry_one, &mut reports).unwrap();
            assert_eq!(reports.len(), 1, "tick should expire order 1");
            assert!(matches!(
                reports[0],
                ExecutionReport::Cancelled {
                    order_id: OrderId(1),
                    ..
                }
            ));
            reports.clear();
        }

        // Recover via full journal replay (no snapshot yet).
        let recovered = TestExchange::recover(&path).unwrap();
        // Order 1 was cancelled before recovery; only order 2 should remain.
        let book_two = recovered
            .exchange()
            .accounts()
            .balance(ACCT_A, USD)
            .reserved;
        assert!(book_two > 0, "order 2 should still be on the book");

        // Drive recovery to the point where order 2 should also expire.
        let mut recovered = recovered;
        let mut replay_reports = Vec::new();
        recovered.tick(expiry_two, &mut replay_reports).unwrap();
        assert_eq!(replay_reports.len(), 1);
        assert!(matches!(
            replay_reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(2),
                ..
            }
        ));
    }

    #[test]
    fn recover_continues_appending() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("continue.journal");

        {
            let mut je = TestExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
            // With hash-chain: Genesis(1) + AddInstrument(2) + Deposit(3) = next is 4.
            // Without: AddInstrument(1) + Deposit(2) = next is 3.
            assert_eq!(je.next_sequence(), FIRST_SEQ + 2);
        }

        // Recover and append more.
        {
            let mut je = TestExchange::recover(&path).unwrap();
            assert_eq!(je.next_sequence(), FIRST_SEQ + 2);

            je.deposit(ACCT_B, BTC, 500).unwrap();
            assert_eq!(je.next_sequence(), FIRST_SEQ + 3);
        }

        // Recover again — should see all 3 user events.
        let je = TestExchange::recover(&path).unwrap();
        assert_eq!(je.next_sequence(), FIRST_SEQ + 3);
        assert_eq!(
            je.exchange().accounts().balance(ACCT_A, USD).available,
            100_000
        );
        assert_eq!(je.exchange().accounts().balance(ACCT_B, BTC).available, 500);
    }

    #[test]
    fn recover_empty_journal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.journal");

        {
            let _je = TestExchange::create(&path).unwrap();
        }

        let je = TestExchange::recover(&path).unwrap();
        // With hash-chain, genesis consumed seq 1, so next is 2; without, next is 1.
        assert_eq!(je.next_sequence(), FIRST_SEQ);
    }

    #[test]
    fn crash_mid_write_recovers_gracefully() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("crash.journal");

        {
            let mut je = TestExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
        }

        // Find valid data end (file is larger due to pre-allocation).
        let valid_data_end = {
            let mut reader = crate::journal::JournalReader::open(&path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            reader.valid_file_end()
        };

        // Simulate crash by truncating 3 bytes from the last valid entry.
        {
            let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            file.set_len(valid_data_end - 3).unwrap();
        }

        // Recovery should replay the first event (AddInstrument) but not the truncated Deposit.
        let je = TestExchange::recover(&path).unwrap();
        // With hash-chain: Genesis(1) + AddInstrument(2) survived, Deposit(3) truncated → next=3.
        // Without: AddInstrument(1) survived, Deposit(2) truncated → next=2.
        assert_eq!(je.next_sequence(), FIRST_SEQ + 1);
        assert_eq!(je.exchange().accounts().balance(ACCT_A, USD).available, 0);
    }

    #[test]
    fn crash_recovery_then_append_no_garbage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no_garbage.journal");

        {
            let mut je = TestExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
        }

        // Find valid data end, then simulate crash by truncating within valid data.
        {
            let valid_data_end = {
                let mut reader = crate::journal::JournalReader::open(&path).unwrap();
                while reader.next_entry().unwrap().is_some() {}
                reader.valid_file_end()
            };
            let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            file.set_len(valid_data_end - 3).unwrap();
        }

        // Recover and append a new event.
        {
            let mut je = TestExchange::recover(&path).unwrap();
            je.deposit(ACCT_A, USD, 50_000).unwrap();
        }

        // Full re-recovery should see both events cleanly (no garbage between).
        let je = TestExchange::recover(&path).unwrap();
        // With hash-chain: Genesis(1) + AddInstrument(2) + Deposit(3) → next=4
        // Without: AddInstrument(1) + Deposit(2) → next=3
        assert_eq!(je.next_sequence(), FIRST_SEQ + 2);
        assert_eq!(
            je.exchange().accounts().balance(ACCT_A, USD).available,
            50_000
        );
    }

    #[test]
    fn snapshot_then_journal_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("snap_journal.journal");
        let snap_path = dir.path().join("snap.snapshot");

        {
            let mut je = TestExchange::create(&journal_path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
            je.deposit(ACCT_B, BTC, 500).unwrap();

            let mut reports = Vec::new();
            je.execute(
                Symbol(1),
                limit_order(1, ACCT_B, Side::Sell, 100, 50),
                &mut reports,
            )
            .unwrap();

            // Save snapshot at this point (genesis=1, 3 user events → seq=4).
            je.save_snapshot(&snap_path).unwrap();

            // More events after snapshot.
            je.execute(
                Symbol(1),
                limit_order(2, ACCT_A, Side::Buy, 100, 20),
                &mut reports,
            )
            .unwrap();
        }

        // Recover from snapshot + journal.
        let je = TestExchange::recover_from_snapshot(&snap_path, &journal_path).unwrap();
        // With hash-chain: Genesis(1) + 4 user events(2,3,4,5) + 1 post-snap(6) → next=7
        // Without: 4 user events(1,2,3,4) + 1 post-snap(5) → next=6
        assert_eq!(je.next_sequence(), FIRST_SEQ + 5);
        // Buyer got 20 BTC from fill.
        assert_eq!(je.exchange().accounts().balance(ACCT_A, BTC).available, 20);
        // Seller still has 30 resting (50 - 20 filled).
        assert_eq!(je.exchange().accounts().balance(ACCT_B, BTC).reserved, 30);
    }

    #[test]
    fn snapshot_recovery_with_zero_chain_hash() {
        // Simulates recovery from a snapshot saved with chain_hash = [0;32]
        // (e.g., an older snapshot before hash chain tracking). Recovery must
        // still produce correct Exchange state by replaying post-snapshot
        // journal entries without chain verification (seed_chain_hash is a
        // no-op for zero hash).
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("zero_hash.journal");
        let snap_path = dir.path().join("zero_hash.snapshot");

        let expected_seq;
        {
            let mut je = TestExchange::create(&journal_path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
            je.deposit(ACCT_B, BTC, 500).unwrap();

            // Save snapshot with zero chain hash (replica scenario).
            let seq = je.next_sequence().saturating_sub(1);
            snapshot::save(je.exchange(), seq, [0u8; 32], &snap_path).unwrap();

            // More events after the zero-hash snapshot.
            let mut reports = Vec::new();
            je.execute(
                Symbol(1),
                limit_order(1, ACCT_B, Side::Sell, 100, 50),
                &mut reports,
            )
            .unwrap();
            je.execute(
                Symbol(1),
                limit_order(2, ACCT_A, Side::Buy, 100, 20),
                &mut reports,
            )
            .unwrap();
            expected_seq = je.next_sequence();
        }

        // Recover from snapshot (zero chain hash) + journal.
        let je = TestExchange::recover_from_snapshot(&snap_path, &journal_path).unwrap();
        assert_eq!(je.next_sequence(), expected_seq);
        // Buyer got 20 BTC from fill.
        assert_eq!(je.exchange().accounts().balance(ACCT_A, BTC).available, 20);
        // Seller: 500 - 50 reserved + 20 filled = 450 available, 30 resting.
        assert_eq!(je.exchange().accounts().balance(ACCT_B, BTC).available, 450);
        assert_eq!(je.exchange().accounts().balance(ACCT_B, BTC).reserved, 30);
    }

    #[test]
    fn journal_replay_restores_circuit_breaker_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cb_replay.journal");

        {
            let mut je = TestExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();

            // Set a trading halt.
            je.set_circuit_breaker(
                Symbol(1),
                CircuitBreakerConfig {
                    halted: true,
                    ..Default::default()
                },
            )
            .unwrap();
        }

        // Recover from journal — halt should be restored.
        let mut recovered = TestExchange::recover(&path).unwrap();
        let mut reports = Vec::new();
        recovered
            .execute(
                Symbol(1),
                limit_order(1, ACCT_A, Side::Buy, 100, 10),
                &mut reports,
            )
            .unwrap();
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::TradingHalted,
                ..
            }
        ));
    }

    #[test]
    fn rotate_produces_valid_snapshot_and_new_journal() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("test.journal");
        let snap_path = dir.path().join("test.snapshot");

        // Create engine, seed data, submit some orders.
        let mut engine = TestExchange::create(&journal_path).unwrap();
        engine.add_instrument(btc_usd_spec()).unwrap();
        engine.deposit(ACCT_A, USD, 1_000_000).unwrap();
        engine.deposit(ACCT_A, BTC, 1_000).unwrap();

        let mut reports = Vec::new();
        engine
            .execute(
                Symbol(1),
                limit_order(1, ACCT_A, Side::Buy, 100, 50),
                &mut reports,
            )
            .unwrap();

        let seq_before = engine.next_sequence();
        let size_before = engine.journal_size();
        assert!(size_before > 0);

        // Rotate.
        engine.rotate(&snap_path).unwrap();

        // Snapshot exists.
        assert!(snap_path.exists());
        // Old journal archived.
        let archived = format!("{}.000001", journal_path.display());
        assert!(std::path::Path::new(&archived).exists());
        // New journal exists and is smaller.
        assert!(journal_path.exists());
        assert!(engine.journal_size() < size_before);
        // Sequence continues. With hash-chain, the new journal writes a
        // genesis entry consuming one sequence number.
        #[cfg(feature = "hash-chain")]
        let rotation_cost = 1u64;
        #[cfg(not(feature = "hash-chain"))]
        let rotation_cost = 0u64;
        assert_eq!(engine.next_sequence(), seq_before + rotation_cost);

        // Can still append to the new journal.
        engine
            .execute(
                Symbol(1),
                limit_order(2, ACCT_A, Side::Sell, 200, 30),
                &mut reports,
            )
            .unwrap();
        assert_eq!(engine.next_sequence(), seq_before + rotation_cost + 1);
    }

    #[test]
    fn recovery_after_rotation_produces_identical_state() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("test.journal");
        let snap_path = dir.path().join("test.snapshot");

        // Build state, rotate, add more events.
        let mut engine = TestExchange::create(&journal_path).unwrap();
        engine.add_instrument(btc_usd_spec()).unwrap();
        engine.deposit(ACCT_A, USD, 1_000_000).unwrap();
        engine.deposit(ACCT_B, BTC, 1_000).unwrap();

        let mut reports = Vec::new();
        engine
            .execute(
                Symbol(1),
                limit_order(1, ACCT_A, Side::Buy, 100, 50),
                &mut reports,
            )
            .unwrap();

        // Rotate — snapshot captures the buy order.
        engine.rotate(&snap_path).unwrap();

        // Submit a sell AFTER rotation — only in the new journal.
        engine
            .execute(
                Symbol(1),
                limit_order(1, ACCT_B, Side::Sell, 100, 20),
                &mut reports,
            )
            .unwrap();

        // Capture balances for comparison.
        let bal_a_usd = engine.exchange().accounts().balance(ACCT_A, USD);
        let bal_b_btc = engine.exchange().accounts().balance(ACCT_B, BTC);
        drop(engine);

        // Recover from snapshot + new journal.
        let recovered = TestExchange::recover_from_snapshot(&snap_path, &journal_path).unwrap();
        assert_eq!(
            recovered.exchange().accounts().balance(ACCT_A, USD),
            bal_a_usd
        );
        assert_eq!(
            recovered.exchange().accounts().balance(ACCT_B, BTC),
            bal_b_btc
        );
    }

    #[test]
    fn multiple_rotations_archive_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("test.journal");
        let snap_path = dir.path().join("test.snapshot");

        let mut engine = TestExchange::create(&journal_path).unwrap();
        engine.add_instrument(btc_usd_spec()).unwrap();
        engine.deposit(ACCT_A, USD, 1_000_000).unwrap();

        // Rotate twice — monotonic naming preserves both archives without
        // shifting (no cascade).
        engine.rotate(&snap_path).unwrap();
        assert!(std::path::Path::new(&format!("{}.000001", journal_path.display())).exists());

        engine.deposit(ACCT_A, BTC, 500).unwrap();
        engine.rotate(&snap_path).unwrap();
        assert!(std::path::Path::new(&format!("{}.000001", journal_path.display())).exists());
        assert!(std::path::Path::new(&format!("{}.000002", journal_path.display())).exists());
        assert!(journal_path.exists());
    }

    #[test]
    fn create_continuing_starts_at_correct_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cont.journal");

        let mut writer = SectorWriter::create_continuing(&path, 42, [0xAA; 32]).unwrap();
        // With hash-chain, genesis consumes seq 42, next is 43.
        // Without hash-chain, no genesis, next is 42.
        #[cfg(feature = "hash-chain")]
        let expected_first = 43u64;
        #[cfg(not(feature = "hash-chain"))]
        let expected_first = 42u64;
        assert_eq!(writer.next_sequence(), expected_first);

        let event = JournalEvent::App(crate::trading_event::TradingEvent::Deposit {
            account: ACCT_A,
            currency: USD,
            amount: 100,
        });
        let seq = writer.append(&event).unwrap();
        assert_eq!(seq, expected_first);
        assert_eq!(writer.next_sequence(), expected_first + 1);

        // Read it back. Genesis is transparent, first user entry starts at expected_first.
        let mut reader = crate::journal::JournalReader::open(&path).unwrap();
        let entry = reader.next_entry().unwrap().unwrap();
        assert_eq!(entry.sequence, expected_first);
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn recovery_preserves_chain_hash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("chain.journal");

        let original_hash;
        {
            let mut je = TestExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
            original_hash = je.writer_chain_hash();
        }

        // Recover and verify chain hash is preserved.
        let recovered = TestExchange::recover(&path).unwrap();
        assert_eq!(recovered.writer_chain_hash(), original_hash);
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn rotation_preserves_chain_continuity() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("rot.journal");
        let snap_path = dir.path().join("rot.snapshot");

        let mut engine = TestExchange::create(&journal_path).unwrap();
        engine.add_instrument(btc_usd_spec()).unwrap();
        engine.deposit(ACCT_A, USD, 100_000).unwrap();

        let hash_before_rotate = engine.writer_chain_hash();
        assert!(hash_before_rotate.is_some());

        engine.rotate(&snap_path).unwrap();

        // After rotation, the new journal's genesis hash is the old chain hash.
        // The new chain hash is computed over the genesis entry (which contains
        // the old chain hash), so it will differ from the pre-rotation hash
        // but the chain is cryptographically linked.
        let hash_after_rotate = engine.writer_chain_hash();
        assert!(hash_after_rotate.is_some());
        // The hash should differ because the genesis entry was hashed.
        assert_ne!(hash_before_rotate, hash_after_rotate);
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn snapshot_stores_chain_hash() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("snap_chain.journal");
        let snap_path = dir.path().join("snap_chain.snapshot");

        let chain_hash_at_snap;
        {
            let mut je = TestExchange::create(&journal_path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
            je.save_snapshot(&snap_path).unwrap();
            chain_hash_at_snap = je.writer_chain_hash().unwrap();
        }

        // Load snapshot and verify chain hash.
        let (_, _, loaded_hash) = snapshot::load(&snap_path).unwrap();
        assert_eq!(loaded_hash, chain_hash_at_snap);
    }

    #[test]
    fn snapshot_recovery_chain_hash_matches_after_rotation() {
        // Exercises the critical path: snapshot recovery with a post-rotation
        // journal. The reader must reinitialize the chain from the genesis
        // entry in the new journal, NOT use the snapshot's chain_hash seed.
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("rot_chain.journal");
        let snap_path = dir.path().join("rot_chain.snapshot");

        // Build state, rotate (creates snapshot + new journal with genesis).
        let mut engine = TestExchange::create(&journal_path).unwrap();
        engine.add_instrument(btc_usd_spec()).unwrap();
        engine.deposit(ACCT_A, USD, 1_000_000).unwrap();
        engine.rotate(&snap_path).unwrap();

        // Append events to the new journal.
        engine.deposit(ACCT_B, BTC, 500).unwrap();
        let mut reports = Vec::new();
        engine
            .execute(
                Symbol(1),
                limit_order(1, ACCT_B, Side::Sell, 100, 50),
                &mut reports,
            )
            .unwrap();
        let writer_hash = engine.writer_chain_hash();
        drop(engine);

        // Recover from snapshot + new journal.
        let recovered = TestExchange::recover_from_snapshot(&snap_path, &journal_path).unwrap();
        // Chain hash must match — this would fail if the reader used the
        // snapshot seed instead of reinitializing from genesis.
        assert_eq!(recovered.writer_chain_hash(), writer_hash);
        assert_eq!(
            recovered
                .exchange()
                .accounts()
                .balance(ACCT_B, BTC)
                .reserved,
            50
        );
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn crash_recovery_chain_hash_continuity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("crash_chain.journal");

        {
            let mut je = TestExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
            je.deposit(ACCT_B, BTC, 500).unwrap();
        }

        // Simulate crash by truncating last entry.
        {
            let mut reader = crate::journal::JournalReader::open(&path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            let valid_end = reader.valid_file_end();
            let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            file.set_len(valid_end - 3).unwrap();
        }

        // Recover — chain should be valid for the surviving entries.
        let mut je = TestExchange::recover(&path).unwrap();
        let hash_after_crash = je.writer_chain_hash();
        assert!(hash_after_crash.is_some());

        // Append more events.
        je.deposit(ACCT_A, BTC, 200).unwrap();
        let hash_after_append = je.writer_chain_hash();
        assert_ne!(hash_after_crash, hash_after_append);
        drop(je);

        // Re-recover — chain should match.
        let je2 = TestExchange::recover(&path).unwrap();
        assert_eq!(je2.writer_chain_hash(), hash_after_append);
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn multiple_rotations_preserve_chain_state() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("multi_rot.journal");
        let snap_path = dir.path().join("multi_rot.snapshot");

        let mut engine = TestExchange::create(&journal_path).unwrap();
        engine.add_instrument(btc_usd_spec()).unwrap();
        engine.deposit(ACCT_A, USD, 1_000_000).unwrap();

        // Three rotations, each with events in between.
        for i in 0..3 {
            engine.deposit(ACCT_A, BTC, (i + 1) * 100).unwrap();
            let hash_before = engine.writer_chain_hash();
            assert!(hash_before.is_some());

            engine.rotate(&snap_path).unwrap();

            // Chain hash should change (genesis entry hashed).
            let hash_after = engine.writer_chain_hash();
            assert!(hash_after.is_some());
            assert_ne!(hash_before, hash_after);
        }

        // Final deposit after all rotations.
        engine.deposit(ACCT_B, BTC, 999).unwrap();
        let final_hash = engine.writer_chain_hash();
        drop(engine);

        // Recovery from latest snapshot + journal should match.
        let recovered = TestExchange::recover_from_snapshot(&snap_path, &journal_path).unwrap();
        assert_eq!(recovered.writer_chain_hash(), final_hash);
        assert_eq!(
            recovered
                .exchange()
                .accounts()
                .balance(ACCT_B, BTC)
                .available,
            999
        );
    }

    #[test]
    fn journal_replay_with_withdraw() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("withdraw.journal");

        {
            let mut je = TestExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
            je.withdraw(ACCT_A, USD, 50_000).unwrap();
        }

        // Replay should produce the same state.
        let je = TestExchange::recover(&path).unwrap();
        assert_eq!(
            je.exchange().accounts().balance(ACCT_A, USD).available,
            50_000
        );
    }

    #[test]
    fn withdraw_insufficient_balance_returns_error() {
        // Regression: JournaledExchange::withdraw used to silently discard
        // the underlying RejectReason and return Ok, hiding failures from
        // the caller. The journaled event must still be recorded (for
        // deterministic replay), but the API must surface the rejection.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("withdraw_err.journal");

        let mut je = TestExchange::create(&path).unwrap();
        je.deposit(ACCT_A, USD, 100).unwrap();

        let err = je.withdraw(ACCT_A, USD, 200).unwrap_err();
        assert!(
            matches!(
                err,
                JournaledExchangeError::Rejected(RejectReason::InsufficientBalance)
            ),
            "expected Rejected(InsufficientBalance), got {err:?}"
        );

        // Balance must be unchanged.
        assert_eq!(je.exchange().accounts().balance(ACCT_A, USD).available, 100);
    }

    #[test]
    fn withdraw_with_resting_orders_returns_error() {
        // A withdraw against an account with resting orders must be rejected
        // and the rejection must be surfaced to the caller. The journal entry
        // is still appended so replay reproduces the same outcome.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("withdraw_resting.journal");

        let mut je = TestExchange::create(&path).unwrap();
        je.add_instrument(btc_usd_spec()).unwrap();
        je.deposit(ACCT_A, USD, 100_000).unwrap();

        let mut reports = Vec::new();
        je.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10),
            &mut reports,
        )
        .unwrap();

        let err = je.withdraw(ACCT_A, USD, 1).unwrap_err();
        assert!(
            matches!(
                err,
                JournaledExchangeError::Rejected(RejectReason::HasRestingOrders)
            ),
            "expected Rejected(HasRestingOrders), got {err:?}"
        );
    }

    #[test]
    fn withdraw_unknown_account_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("withdraw_unknown.journal");
        let mut je = TestExchange::create(&path).unwrap();
        let err = je.withdraw(ACCT_A, USD, 1).unwrap_err();
        assert!(
            matches!(
                err,
                JournaledExchangeError::Rejected(RejectReason::UnknownAccount)
            ),
            "expected Rejected(UnknownAccount), got {err:?}"
        );
    }

    #[test]
    fn journal_replay_rejected_withdraw_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("withdraw_rejected.journal");

        {
            let mut je = TestExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();

            // Place a resting order, then attempt withdraw (should fail).
            let mut reports = Vec::new();
            je.execute(
                Symbol(1),
                limit_order(1, ACCT_A, Side::Buy, 100, 10),
                &mut reports,
            )
            .unwrap();

            // This withdraw is journaled but rejected at execution because
            // the account has a resting order. The error must be surfaced.
            let err = je.withdraw(ACCT_A, USD, 1_000).unwrap_err();
            assert!(
                matches!(
                    err,
                    JournaledExchangeError::Rejected(RejectReason::HasRestingOrders)
                ),
                "expected Rejected(HasRestingOrders), got {err:?}"
            );
        }

        // Replay: the rejected withdraw should be a no-op.
        let je = TestExchange::recover(&path).unwrap();
        // Balance: 100K deposited, 1000 reserved by order, withdraw rejected.
        assert_eq!(
            je.exchange().accounts().balance(ACCT_A, USD).available,
            99_000
        );
        assert_eq!(
            je.exchange().accounts().balance(ACCT_A, USD).reserved,
            1_000
        );
    }

    #[test]
    fn snapshot_round_trip_with_withdrawals() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("snap_withdraw.journal");
        let snap_path = dir.path().join("snap_withdraw.snapshot");

        {
            let mut je = TestExchange::create(&journal_path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
            je.deposit(ACCT_A, BTC, 500).unwrap();
            je.deposit(ACCT_B, USD, 50_000).unwrap();
            je.deposit(ACCT_B, BTC, 200).unwrap();

            // Trade: B sells 10 BTC to A at 100.
            let mut reports = Vec::new();
            je.execute(
                Symbol(1),
                limit_order(1, ACCT_B, Side::Sell, 100, 10),
                &mut reports,
            )
            .unwrap();
            reports.clear();
            je.execute(
                Symbol(1),
                limit_order(1, ACCT_A, Side::Buy, 100, 10),
                &mut reports,
            )
            .unwrap();

            // Withdraw all of ACCT_B's USD (50_000 original + 1_000 from sale).
            let b_usd = je.exchange().accounts().balance(ACCT_B, USD).available;
            je.withdraw(ACCT_B, USD, b_usd).unwrap();

            // Snapshot + rotate.
            je.rotate(&snap_path).unwrap();

            // One more deposit after rotation.
            je.deposit(ACCT_A, USD, 999).unwrap();
        }

        // Recovery from snapshot + journal.
        let je = TestExchange::recover_from_snapshot(&snap_path, &journal_path).unwrap();

        // ACCT_A: 100K - 1000 (bought 10 @ 100) + 999 = 99_999 USD, 500 + 10 = 510 BTC
        assert_eq!(
            je.exchange().accounts().balance(ACCT_A, USD).available,
            99_999
        );
        assert_eq!(je.exchange().accounts().balance(ACCT_A, BTC).available, 510);

        // ACCT_B: 0 USD (withdrawn), 200 - 10 = 190 BTC
        assert_eq!(je.exchange().accounts().balance(ACCT_B, USD).available, 0);
        assert_eq!(je.exchange().accounts().balance(ACCT_B, BTC).available, 190);
    }

    #[test]
    fn key_hwm_survives_journal_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let key_hash: u64 = 0xCAFE;

        // Write journal entries with key_hash + request_seq.
        {
            let mut writer = crate::journal::SectorWriter::create(&path).unwrap();
            let ts = crate::journal::wall_clock_nanos();
            // Deposit with seq=1
            writer
                .batch_append_with_ts(
                    &JournalEvent::App(crate::trading_event::TradingEvent::AddInstrument {
                        spec: btc_usd_spec(),
                    }),
                    ts,
                    key_hash,
                    1,
                )
                .unwrap();
            // Deposit with seq=2
            writer
                .batch_append_with_ts(
                    &JournalEvent::App(crate::trading_event::TradingEvent::Deposit {
                        account: ACCT_A,
                        currency: USD,
                        amount: 1000,
                    }),
                    ts,
                    key_hash,
                    2,
                )
                .unwrap();
            writer.flush_batch_sync().unwrap();
        }

        // Recover should rebuild the HWM.
        let je = TestExchange::recover(&path).unwrap();
        let exchange = je.exchange();

        // The HWM for key_hash should be 2.
        // Verify by checking that seq=2 would be rejected and seq=3 accepted.
        let mut ex_clone = Exchange::new();
        ex_clone.add_instrument(btc_usd_spec());
        // Manually rebuild: check_request_seq(key_hash, 1) then (key_hash, 2)
        // should advance HWM to 2.
        // Since we can't directly read key_hwm, verify through snapshot.
        let hwm_snap = exchange.snapshot_key_hwm();
        assert_eq!(hwm_snap.len(), 1);
        assert_eq!(hwm_snap[0], (key_hash, 2));
    }

    #[test]
    fn key_hwm_survives_snapshot_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("test.journal");
        let snapshot_path = dir.path().join("test.snap");

        let key_hash: u64 = 0xBEEF;

        // Create journaled exchange, write events with key_hash.
        {
            let mut writer = crate::journal::SectorWriter::create(&journal_path).unwrap();
            let ts = crate::journal::wall_clock_nanos();
            writer
                .batch_append_with_ts(
                    &JournalEvent::App(crate::trading_event::TradingEvent::AddInstrument {
                        spec: btc_usd_spec(),
                    }),
                    ts,
                    key_hash,
                    1,
                )
                .unwrap();
            writer
                .batch_append_with_ts(
                    &JournalEvent::App(crate::trading_event::TradingEvent::Deposit {
                        account: ACCT_A,
                        currency: USD,
                        amount: 5000,
                    }),
                    ts,
                    key_hash,
                    5,
                )
                .unwrap();
            writer.flush_batch_sync().unwrap();
        }

        // Recover and snapshot.
        let je = TestExchange::recover(&journal_path).unwrap();
        je.save_snapshot(&snapshot_path).unwrap();

        // Load snapshot into a fresh exchange.
        let (restored, _seq, _chain) = super::snapshot::load(&snapshot_path).unwrap();
        let hwm = restored.snapshot_key_hwm();
        assert_eq!(hwm.len(), 1);
        assert_eq!(hwm[0], (key_hash, 5));
    }

    // ---------------------------------------------------------------
    // Crash injection tests
    // ---------------------------------------------------------------

    /// Helper: copy a file byte-for-byte so truncation tests don't destroy the original.
    fn copy_file(src: &Path, dst: &Path) {
        std::fs::copy(src, dst).unwrap();
    }

    /// Helper: find the byte offset where valid journal data ends
    /// (after the last fully-written entry, before pre-allocated space).
    fn valid_data_end(path: &Path) -> u64 {
        let mut reader = crate::journal::JournalReader::open(path).unwrap();
        while reader.next_entry().unwrap().is_some() {}
        reader.valid_file_end()
    }

    /// Helper: build a non-trivial exchange state with multiple event types.
    /// Returns the number of user-visible events written.
    fn build_workload(je: &mut TestExchange) -> usize {
        let mut reports = Vec::new();
        let mut count = 0;

        // Instrument + deposits.
        je.add_instrument(btc_usd_spec()).unwrap();
        count += 1;
        je.deposit(ACCT_A, USD, 10_000_000).unwrap();
        count += 1;
        je.deposit(ACCT_B, BTC, 10_000).unwrap();
        count += 1;
        je.deposit(ACCT_B, USD, 5_000_000).unwrap();
        count += 1;

        // Risk limits.
        je.set_risk_limits(
            Symbol(1),
            RiskLimits {
                max_order_qty: Some(qty(5000)),
                max_order_notional: Some(500_000_000),
            },
        )
        .unwrap();
        count += 1;

        // Circuit breaker.
        je.set_circuit_breaker(
            Symbol(1),
            CircuitBreakerConfig {
                price_band_lower: Some(price(50)),
                price_band_upper: Some(price(200)),
                halted: false,
            },
        )
        .unwrap();
        count += 1;

        // Resting sells from ACCT_B.
        for i in 1..=10 {
            je.execute(
                Symbol(1),
                limit_order(i, ACCT_B, Side::Sell, 100 + i, 100),
                &mut reports,
            )
            .unwrap();
            count += 1;
        }

        // Resting buys from ACCT_A.
        for i in 11..=20 {
            je.execute(
                Symbol(1),
                limit_order(i, ACCT_A, Side::Buy, 90 - (i - 11), 50),
                &mut reports,
            )
            .unwrap();
            count += 1;
        }

        // Aggressive buy that fills against lowest ask.
        je.execute(
            Symbol(1),
            limit_order(21, ACCT_A, Side::Buy, 101, 30),
            &mut reports,
        )
        .unwrap();
        count += 1;

        // Cancel some resting orders.
        for id in [12, 14, 16] {
            je.cancel(Symbol(1), ACCT_A, OrderId(id), &mut reports)
                .unwrap();
            count += 1;
        }

        // Withdraw — must cancel resting orders first, otherwise the
        // withdraw is rejected with HasRestingOrders.
        je.cancel_all(ACCT_A, &mut reports).unwrap();
        count += 1;
        je.withdraw(ACCT_A, USD, 1_000).unwrap();
        count += 1;

        count
    }

    /// Exhaustive crash simulation: truncate the journal at *every* byte offset
    /// from the file header through the valid data end. For each truncation,
    /// verify that recovery succeeds and the engine can continue appending.
    ///
    /// Uses a small workload (5 events) to keep the byte range manageable
    /// (~500 bytes → ~500 iterations). The larger workload is exercised
    /// by `crash_recovery_under_realistic_load` with sampled truncation points.
    #[test]
    fn crash_at_every_byte_offset_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let original = dir.path().join("original.journal");

        {
            let mut je = TestExchange::create(&original).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
            je.deposit(ACCT_B, BTC, 500).unwrap();
            let mut reports = Vec::new();
            je.execute(
                Symbol(1),
                limit_order(1, ACCT_B, Side::Sell, 100, 50),
                &mut reports,
            )
            .unwrap();
            je.execute(
                Symbol(1),
                limit_order(2, ACCT_A, Side::Buy, 100, 30),
                &mut reports,
            )
            .unwrap();
        }

        let end = valid_data_end(&original);
        let header_end = melin_journal::codec::FILE_HEADER_SIZE as u64;
        assert!(end > header_end, "journal should have data beyond header");

        let work = dir.path().join("work.journal");

        // Shrink the original to its valid data size. The pre-allocated file
        // is 256 MiB; copying that per iteration dominates runtime.
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&original)
                .unwrap();
            f.set_len(end).unwrap();
        }

        for trunc_at in header_end..=end {
            // Copy the (now small) original and truncate.
            copy_file(&original, &work);
            {
                let f = std::fs::OpenOptions::new().write(true).open(&work).unwrap();
                f.set_len(trunc_at).unwrap();
            }

            // Recovery must not panic or error.
            let mut je = TestExchange::recover(&work).unwrap();

            // Sequence must be valid (at least 1, the starting point for an empty journal).
            assert!(je.next_sequence() >= 1, "seq underflow at byte {trunc_at}");

            // Must be able to append after recovery.
            je.deposit(ACCT_A, USD, 1).unwrap();

            // Double-recovery of the post-append journal must also succeed.
            drop(je);
            let je2 = TestExchange::recover(&work).unwrap();
            // At least 2: the deposit we appended (seq 1) + next is 2.
            assert!(
                je2.next_sequence() >= 2,
                "double-recovery seq too low at byte {trunc_at}"
            );
        }
    }

    /// Crash during or after snapshot-based rotation: truncate the *new*
    /// journal at every byte, recover from snapshot + truncated journal.
    /// Also tests the "journal missing after rotation" edge case.
    #[test]
    fn crash_during_snapshot_rotation_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("rotation.journal");
        let snap_path = dir.path().join("rotation.snapshot");

        // Build state, rotate, add more events after rotation.
        let mut engine = TestExchange::create(&journal_path).unwrap();
        build_workload(&mut engine);
        engine.rotate(&snap_path).unwrap();

        // Post-rotation events.
        let mut reports = Vec::new();
        engine.deposit(ACCT_A, USD, 777).unwrap();
        engine
            .execute(
                Symbol(1),
                limit_order(100, ACCT_A, Side::Buy, 80, 10),
                &mut reports,
            )
            .unwrap();
        let final_seq = engine.next_sequence();
        drop(engine);

        // Snapshot state (for comparison when all post-rotation events are lost).
        let (snap_exchange, snap_seq, _snap_hash) = super::snapshot::load(&snap_path).unwrap();
        let snap_bal_a_usd = snap_exchange.accounts().balance(ACCT_A, USD).available;

        let end = valid_data_end(&journal_path);
        let header_end = melin_journal::codec::FILE_HEADER_SIZE as u64;

        let work_journal = dir.path().join("work.journal");

        // Shrink the post-rotation journal to its valid data size to
        // avoid copying the 256 MiB pre-allocated file each iteration.
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&journal_path)
                .unwrap();
            f.set_len(end).unwrap();
        }

        // Truncate at every byte in the new (post-rotation) journal.
        for trunc_at in header_end..=end {
            copy_file(&journal_path, &work_journal);
            {
                let f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&work_journal)
                    .unwrap();
                f.set_len(trunc_at).unwrap();
            }

            let je = TestExchange::recover_from_snapshot(&snap_path, &work_journal).unwrap();

            // Sequence must be between snapshot seq and final seq (inclusive of snap+1
            // because the new journal's genesis consumes one seq with hash-chain).
            assert!(
                je.next_sequence() <= final_seq,
                "seq overshoot at byte {trunc_at}: {} > {final_seq}",
                je.next_sequence()
            );
            assert!(
                je.next_sequence() > snap_seq,
                "seq undershot snapshot at byte {trunc_at}"
            );
        }

        // Edge case: journal file missing entirely after rotation.
        // Recovery should succeed from snapshot alone.
        std::fs::remove_file(&journal_path).ok();
        // The server's init_engine handles this case by loading the snapshot
        // and creating a fresh journal. Simulate that path here.
        let (exchange, seq, chain_hash) = super::snapshot::load(&snap_path).unwrap();
        let writer = BufferedWriter::create_continuing(&journal_path, seq + 1, chain_hash).unwrap();
        let je = JournaledExchange::from_parts(exchange, writer);
        assert_eq!(
            je.exchange().accounts().balance(ACCT_A, USD).available,
            snap_bal_a_usd
        );
    }

    /// Replay remaining events after crash to verify deterministic recovery.
    ///
    /// Strategy: write N events, record each event + the journal byte offset
    /// after it. For a sample of truncation points, recover, replay the
    /// remaining events on the recovered exchange, and verify the final state
    /// matches the reference.
    #[test]
    fn crash_recovery_under_realistic_load() {
        let dir = tempfile::tempdir().unwrap();
        let original = dir.path().join("realistic.journal");

        // Collect events so we can replay them individually.
        let events = build_event_list();

        // Write all events, recording the valid-data-end after each one.
        let mut checkpoints: Vec<(u64, u64)> = Vec::new(); // (seq_after, file_pos_after)
        {
            let mut je = TestExchange::create(&original).unwrap();
            let mut reports = Vec::new();
            for evt in &events {
                apply_event(&mut je, evt, &mut reports);
                reports.clear();
                // Flush to get stable file position.
                checkpoints.push((je.next_sequence(), valid_data_end(&original)));
            }
        }

        // Reference state: recover from the full journal.
        let reference = TestExchange::recover(&original).unwrap();
        let ref_bal_a_usd = reference
            .exchange()
            .accounts()
            .balance(ACCT_A, USD)
            .available;
        let ref_bal_a_btc = reference
            .exchange()
            .accounts()
            .balance(ACCT_A, BTC)
            .available;
        let ref_bal_b_usd = reference
            .exchange()
            .accounts()
            .balance(ACCT_B, USD)
            .available;
        let ref_bal_b_btc = reference
            .exchange()
            .accounts()
            .balance(ACCT_B, BTC)
            .available;
        let ref_seq = reference.next_sequence();
        drop(reference);

        // Sample truncation points: every 5th event boundary.
        // Shrink to valid data size to avoid copying 256 MiB per iteration.
        let original_end = valid_data_end(&original);
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&original)
                .unwrap();
            f.set_len(original_end).unwrap();
        }

        let work = dir.path().join("work.journal");
        for (i, &(_seq, file_pos)) in checkpoints.iter().enumerate().step_by(5) {
            copy_file(&original, &work);
            {
                let f = std::fs::OpenOptions::new().write(true).open(&work).unwrap();
                f.set_len(file_pos).unwrap();
            }

            // Recover from truncated journal.
            let mut je = TestExchange::recover(&work).unwrap();
            let _recovered_seq = je.next_sequence();

            // Replay the remaining events (those after the truncation point).
            // Events 0..=i were written; event i is the last one fully on disk
            // at file_pos. We need to replay events starting from i+1.
            let mut reports = Vec::new();
            for evt in &events[(i + 1)..] {
                apply_event(&mut je, evt, &mut reports);
                reports.clear();
            }

            // Final state must match reference.
            assert_eq!(
                je.exchange().accounts().balance(ACCT_A, USD).available,
                ref_bal_a_usd,
                "ACCT_A USD mismatch after crash at event {i}"
            );
            assert_eq!(
                je.exchange().accounts().balance(ACCT_A, BTC).available,
                ref_bal_a_btc,
                "ACCT_A BTC mismatch after crash at event {i}"
            );
            assert_eq!(
                je.exchange().accounts().balance(ACCT_B, USD).available,
                ref_bal_b_usd,
                "ACCT_B USD mismatch after crash at event {i}"
            );
            assert_eq!(
                je.exchange().accounts().balance(ACCT_B, BTC).available,
                ref_bal_b_btc,
                "ACCT_B BTC mismatch after crash at event {i}"
            );
            assert_eq!(
                je.next_sequence(),
                ref_seq,
                "sequence mismatch after crash at event {i}"
            );
            // Note: chain hash is NOT compared here because re-appended events
            // have different wall-clock timestamps, producing different hashes.
            // Chain hash integrity is validated by the other crash tests that
            // don't re-append (they only verify recovery succeeds).
        }
    }

    /// Verify every state type survives crash recovery.
    #[test]
    fn crash_recovery_preserves_all_state_types() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("all_state.journal");

        let mut reports = Vec::new();
        {
            let mut je = TestExchange::create(&path).unwrap();

            // Instrument.
            je.add_instrument(btc_usd_spec()).unwrap();

            // Deposits.
            je.deposit(ACCT_A, USD, 10_000_000).unwrap();
            je.deposit(ACCT_B, BTC, 10_000).unwrap();
            je.deposit(ACCT_B, USD, 5_000_000).unwrap();

            // Risk limits.
            je.set_risk_limits(
                Symbol(1),
                RiskLimits {
                    max_order_qty: Some(qty(9999)),
                    max_order_notional: Some(123_456_789),
                },
            )
            .unwrap();

            // Circuit breaker.
            je.set_circuit_breaker(
                Symbol(1),
                CircuitBreakerConfig {
                    price_band_lower: Some(price(10)),
                    price_band_upper: Some(price(500)),
                    halted: false,
                },
            )
            .unwrap();

            // Resting orders (buy side + sell side).
            je.execute(
                Symbol(1),
                limit_order(1, ACCT_A, Side::Buy, 95, 200),
                &mut reports,
            )
            .unwrap();
            je.execute(
                Symbol(1),
                limit_order(1, ACCT_B, Side::Sell, 105, 300),
                &mut reports,
            )
            .unwrap();

            // Capture reference state.
            reports.clear();
        }

        // Recover and verify every state type.
        let je = TestExchange::recover(&path).unwrap();
        let ex = je.exchange();

        // Balances.
        // ACCT_A: 10M USD deposited, 95*200 = 19000 reserved for buy.
        assert_eq!(
            ex.accounts().balance(ACCT_A, USD).available,
            10_000_000 - 19_000
        );
        assert_eq!(ex.accounts().balance(ACCT_A, USD).reserved, 19_000);
        assert_eq!(ex.accounts().balance(ACCT_B, BTC).available, 10_000 - 300);
        assert_eq!(ex.accounts().balance(ACCT_B, BTC).reserved, 300);

        // Risk limits.
        let rl = ex.snapshot_risk_limits();
        assert_eq!(rl.len(), 1);
        assert_eq!(rl[0].0, Symbol(1));
        assert_eq!(rl[0].1.max_order_qty, Some(qty(9999)));
        assert_eq!(rl[0].1.max_order_notional, Some(123_456_789));

        // Circuit breaker.
        let cb = ex.snapshot_circuit_breakers();
        assert_eq!(cb.len(), 1);
        assert_eq!(cb[0].0, Symbol(1));
        assert_eq!(cb[0].1.price_band_lower, Some(price(10)));
        assert_eq!(cb[0].1.price_band_upper, Some(price(500)));
        assert!(!cb[0].1.halted);

        // Resting orders exist on both sides.
        let order_sides = ex.snapshot_order_sides();
        let has_buy = order_sides.iter().any(|(_, s)| *s == Side::Buy);
        let has_sell = order_sides.iter().any(|(_, s)| *s == Side::Sell);
        assert!(has_buy, "buy orders should be present");
        assert!(has_sell, "sell orders should be present");

        // Instrument registered.
        let specs: Vec<_> = ex.instrument_specs().collect();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].symbol, Symbol(1));
    }

    /// Crash recovery across multiple rotations: build state over 3 rotation
    /// cycles, truncate the final journal segment at various points, and
    /// verify recovery from the latest snapshot + truncated journal.
    #[test]
    fn multiple_rotation_crash_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("multi_rot.journal");
        let snap_path = dir.path().join("multi_rot.snapshot");

        let mut reports = Vec::new();
        let mut engine = TestExchange::create(&journal_path).unwrap();
        engine.add_instrument(btc_usd_spec()).unwrap();
        engine.deposit(ACCT_A, USD, 10_000_000).unwrap();
        engine.deposit(ACCT_B, BTC, 10_000).unwrap();

        // Rotation 1.
        engine
            .execute(
                Symbol(1),
                limit_order(1, ACCT_B, Side::Sell, 100, 50),
                &mut reports,
            )
            .unwrap();
        engine.rotate(&snap_path).unwrap();

        // Rotation 2.
        engine
            .execute(
                Symbol(1),
                limit_order(2, ACCT_A, Side::Buy, 100, 20),
                &mut reports,
            )
            .unwrap();
        engine.deposit(ACCT_A, BTC, 500).unwrap();
        engine.rotate(&snap_path).unwrap();

        // Rotation 3.
        engine.deposit(ACCT_B, USD, 999).unwrap();
        engine
            .execute(
                Symbol(1),
                limit_order(3, ACCT_A, Side::Buy, 100, 10),
                &mut reports,
            )
            .unwrap();
        engine.rotate(&snap_path).unwrap();

        // Post-rotation events in the final journal segment.
        engine.deposit(ACCT_A, USD, 111).unwrap();
        engine.deposit(ACCT_B, BTC, 222).unwrap();
        engine
            .execute(
                Symbol(1),
                limit_order(4, ACCT_B, Side::Sell, 110, 100),
                &mut reports,
            )
            .unwrap();

        // Reference state.
        let ref_bal_a_usd = engine.exchange().accounts().balance(ACCT_A, USD).available;
        let ref_bal_b_btc = engine.exchange().accounts().balance(ACCT_B, BTC).available;
        let final_seq = engine.next_sequence();
        drop(engine);

        let end = valid_data_end(&journal_path);
        let header_end = melin_journal::codec::FILE_HEADER_SIZE as u64;

        let work = dir.path().join("work_multi.journal");

        // Shrink to valid data size to avoid copying 256 MiB per iteration.
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&journal_path)
                .unwrap();
            f.set_len(end).unwrap();
        }

        // Truncate at every 10th byte to keep runtime reasonable.
        let mut trunc_at = header_end;
        while trunc_at <= end {
            copy_file(&journal_path, &work);
            {
                let f = std::fs::OpenOptions::new().write(true).open(&work).unwrap();
                f.set_len(trunc_at).unwrap();
            }

            let je = TestExchange::recover_from_snapshot(&snap_path, &work).unwrap();

            // Sequence must not exceed the full reference.
            assert!(
                je.next_sequence() <= final_seq,
                "seq overshoot at byte {trunc_at}"
            );

            // Must be able to append after recovery.
            drop(je);
            let mut je2 = TestExchange::recover_from_snapshot(&snap_path, &work).unwrap();
            je2.deposit(ACCT_A, USD, 1).unwrap();

            trunc_at += 10;
        }

        // Full recovery (no truncation) must match reference.
        let full = TestExchange::recover_from_snapshot(&snap_path, &journal_path).unwrap();
        assert_eq!(
            full.exchange().accounts().balance(ACCT_A, USD).available,
            ref_bal_a_usd
        );
        assert_eq!(
            full.exchange().accounts().balance(ACCT_B, BTC).available,
            ref_bal_b_btc
        );
    }

    // -- Helpers for crash_recovery_under_realistic_load --

    /// A journalable event for replay testing.
    enum TestEvent {
        AddInstrument(InstrumentSpec),
        Deposit(AccountId, CurrencyId, u64),
        Submit(Symbol, Order),
        Cancel(Symbol, AccountId, OrderId),
        SetRiskLimits(Symbol, RiskLimits),
        SetCircuitBreaker(Symbol, CircuitBreakerConfig),
        Withdraw(AccountId, CurrencyId, u64),
    }

    /// Build a deterministic list of events exercising multiple code paths.
    fn build_event_list() -> Vec<TestEvent> {
        let mut events = vec![
            // Setup.
            TestEvent::AddInstrument(btc_usd_spec()),
            TestEvent::Deposit(ACCT_A, USD, 50_000_000),
            TestEvent::Deposit(ACCT_B, BTC, 100_000),
            TestEvent::Deposit(ACCT_B, USD, 10_000_000),
            // Config.
            TestEvent::SetRiskLimits(
                Symbol(1),
                RiskLimits {
                    max_order_qty: Some(qty(10_000)),
                    max_order_notional: None,
                },
            ),
            TestEvent::SetCircuitBreaker(
                Symbol(1),
                CircuitBreakerConfig {
                    price_band_lower: Some(price(1)),
                    price_band_upper: Some(price(10_000)),
                    halted: false,
                },
            ),
        ];

        // Build up the order book: 20 sell levels, 20 buy levels.
        for i in 1..=20 {
            events.push(TestEvent::Submit(
                Symbol(1),
                limit_order(i, ACCT_B, Side::Sell, 100 + i, 500),
            ));
        }
        for i in 21..=40 {
            events.push(TestEvent::Submit(
                Symbol(1),
                limit_order(i, ACCT_A, Side::Buy, 100 - (i - 20), 300),
            ));
        }

        // Aggressive orders that fill.
        for i in 41..=50 {
            events.push(TestEvent::Submit(
                Symbol(1),
                limit_order(i, ACCT_A, Side::Buy, 101, 10),
            ));
        }

        // Cancels.
        for id in [22, 25, 28, 31, 34, 37] {
            events.push(TestEvent::Cancel(Symbol(1), ACCT_A, OrderId(id)));
        }

        // Withdrawals.
        events.push(TestEvent::Withdraw(ACCT_A, USD, 1_000));
        events.push(TestEvent::Withdraw(ACCT_B, BTC, 500));

        // More orders after cancels.
        for i in 51..=60 {
            events.push(TestEvent::Submit(
                Symbol(1),
                limit_order(i, ACCT_A, Side::Buy, 85 + (i - 51), 100),
            ));
        }

        events
    }

    /// Apply a TestEvent to a JournaledExchange.
    fn apply_event(je: &mut TestExchange, evt: &TestEvent, reports: &mut Vec<ExecutionReport>) {
        match evt {
            TestEvent::AddInstrument(spec) => {
                je.add_instrument(*spec).unwrap();
            }
            TestEvent::Deposit(acct, cur, amt) => {
                je.deposit(*acct, *cur, *amt).unwrap();
            }
            TestEvent::Submit(sym, order) => {
                je.execute(*sym, *order, reports).unwrap();
            }
            TestEvent::Cancel(sym, acct, oid) => {
                je.cancel(*sym, *acct, *oid, reports).unwrap();
            }
            TestEvent::SetRiskLimits(sym, lim) => {
                je.set_risk_limits(*sym, *lim).unwrap();
            }
            TestEvent::SetCircuitBreaker(sym, cfg) => {
                je.set_circuit_breaker(*sym, *cfg).unwrap();
            }
            TestEvent::Withdraw(acct, cur, amt) => {
                // Random workload may legitimately hit rejections
                // (insufficient balance, resting orders); replay must
                // still reproduce them deterministically.
                match je.withdraw(*acct, *cur, *amt) {
                    Ok(()) | Err(JournaledExchangeError::Rejected(_)) => {}
                    Err(e) => panic!("unexpected journal error: {e:?}"),
                }
            }
        }
    }

    /// SetFeeSchedule events survive journal replay: fees set before a fill
    /// produce the same fee account balance after recovery.
    #[test]
    fn journal_replay_preserves_fee_schedule() {
        use crate::account::FEE_ACCOUNT;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fees.journal");

        let fee_account_balance;
        let acct_a_usd;
        let acct_b_usd;
        {
            let mut je = TestExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
            je.deposit(ACCT_B, BTC, 100).unwrap();

            let mut reports = Vec::new();

            // Set fees BEFORE the fill.
            je.set_fee_schedule(
                Symbol(1),
                FeeSchedule {
                    maker_fee_bps: 10,
                    taker_fee_bps: 20,
                },
                &mut reports,
            )
            .unwrap();
            // Maker sell.
            je.execute(
                Symbol(1),
                limit_order(1, ACCT_B, Side::Sell, 1000, 10),
                &mut reports,
            )
            .unwrap();
            // Taker buy → fills with fees.
            je.execute(
                Symbol(1),
                limit_order(1, ACCT_A, Side::Buy, 1000, 10),
                &mut reports,
            )
            .unwrap();

            fee_account_balance = je.exchange().accounts().balance(FEE_ACCOUNT, USD).available;
            acct_a_usd = je.exchange().accounts().balance(ACCT_A, USD);
            acct_b_usd = je.exchange().accounts().balance(ACCT_B, USD);

            // Under A: seller (maker) quote fee = cost × 10 bps = 10 USD.
            // buyer (taker) base fee = qty × 20 bps = 0 (truncates at this qty).
            assert_eq!(fee_account_balance, 10);
        }

        // Recover from journal and verify fee schedule was replayed.
        let recovered = TestExchange::recover(&path).unwrap();
        assert_eq!(
            recovered
                .exchange()
                .accounts()
                .balance(FEE_ACCOUNT, USD)
                .available,
            fee_account_balance,
            "fee account balance must match after journal replay"
        );
        assert_eq!(
            recovered.exchange().accounts().balance(ACCT_A, USD),
            acct_a_usd,
        );
        assert_eq!(
            recovered.exchange().accounts().balance(ACCT_B, USD),
            acct_b_usd,
        );
    }
}
