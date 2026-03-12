//! JournaledExchange — wraps `Exchange` with durable event journaling.
//!
//! Journals every input command before executing it, ensuring the
//! persist-before-ack invariant. On crash, replay reconstructs identical state.

use std::path::Path;

use crate::exchange::Exchange;
use crate::types::{
    AccountId, CurrencyId, ExecutionReport, InstrumentSpec, Order, OrderId, Symbol,
};

use super::error::JournalError;
use super::event::JournalEvent;
use super::reader::JournalReader;
use super::snapshot;
use super::writer::JournalWriter;

/// Exchange wrapper that journals all input commands to a write-ahead log
/// before executing them. Provides crash recovery via journal replay.
pub struct JournaledExchange {
    exchange: Exchange,
    writer: JournalWriter,
}

impl JournaledExchange {
    /// Create a new journaled exchange with a fresh journal file.
    pub fn create(journal_path: &Path) -> Result<Self, JournalError> {
        let writer = JournalWriter::create(journal_path)?;
        Ok(Self {
            exchange: Exchange::new(),
            writer,
        })
    }

    /// Register a new instrument. Journals before executing.
    pub fn add_instrument(&mut self, spec: InstrumentSpec) -> Result<(), JournalError> {
        self.writer.append(&JournalEvent::AddInstrument { spec })?;
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
        self.writer.append(&JournalEvent::Deposit {
            account,
            currency,
            amount,
        })?;
        self.exchange.deposit(account, currency, amount);
        Ok(())
    }

    /// Submit an order. Journals before executing.
    pub fn execute(
        &mut self,
        symbol: Symbol,
        order: Order,
        reports: &mut Vec<ExecutionReport>,
    ) -> Result<(), JournalError> {
        self.writer
            .append(&JournalEvent::SubmitOrder { symbol, order })?;
        self.exchange.execute(symbol, order, reports);
        Ok(())
    }

    /// Cancel an order. Journals before executing.
    pub fn cancel(
        &mut self,
        symbol: Symbol,
        order_id: OrderId,
        reports: &mut Vec<ExecutionReport>,
    ) -> Result<(), JournalError> {
        self.writer
            .append(&JournalEvent::CancelOrder { symbol, order_id })?;
        self.exchange.cancel(symbol, order_id, reports);
        Ok(())
    }

    /// Recover from an existing journal file by replaying all events.
    ///
    /// Truncates any trailing garbage from a partial write (crash recovery),
    /// then reopens the writer for appending new events.
    pub fn recover(journal_path: &Path) -> Result<Self, JournalError> {
        let mut reader = JournalReader::open(journal_path)?;
        let mut exchange = Exchange::new();
        let mut reports = Vec::new();

        while let Some(entry) = reader.next_entry()? {
            replay_event(&mut exchange, &entry.event, &mut reports);
            reports.clear();
        }

        let last_seq = reader.last_sequence().unwrap_or(0);
        let valid_end = reader.valid_file_end();
        let writer = JournalWriter::open_append(journal_path, last_seq, valid_end)?;

        Ok(Self { exchange, writer })
    }

    /// Recover from a snapshot plus a journal file.
    ///
    /// Loads the snapshot to restore state, then replays only the journal
    /// entries after the snapshot's sequence number. This avoids replaying
    /// the full journal from genesis.
    pub fn recover_from_snapshot(
        snapshot_path: &Path,
        journal_path: &Path,
    ) -> Result<Self, JournalError> {
        let (mut exchange, snap_sequence) = snapshot::load(snapshot_path)?;
        let mut reader = JournalReader::open(journal_path)?;
        let mut reports = Vec::new();

        // Skip entries already captured by the snapshot.
        while let Some(entry) = reader.next_entry()? {
            if entry.sequence > snap_sequence {
                replay_event(&mut exchange, &entry.event, &mut reports);
                reports.clear();
            }
        }

        let last_seq = reader.last_sequence().unwrap_or(snap_sequence);
        let valid_end = reader.valid_file_end();
        let writer = JournalWriter::open_append(journal_path, last_seq, valid_end)?;

        Ok(Self { exchange, writer })
    }

    /// Save a snapshot of the current exchange state.
    ///
    /// The snapshot records the current journal sequence so recovery knows
    /// where to start replaying.
    pub fn save_snapshot(&self, snapshot_path: &Path) -> Result<(), JournalError> {
        // Snapshot captures state as of the last journaled event.
        let seq = self.writer.next_sequence().saturating_sub(1);
        snapshot::save(&self.exchange, seq, snapshot_path)
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
}

/// Replay a single journal event into an exchange. Used during recovery.
fn replay_event(exchange: &mut Exchange, event: &JournalEvent, reports: &mut Vec<ExecutionReport>) {
    match *event {
        JournalEvent::AddInstrument { spec } => {
            exchange.add_instrument(spec);
        }
        JournalEvent::Deposit {
            account,
            currency,
            amount,
        } => {
            exchange.deposit(account, currency, amount);
        }
        JournalEvent::SubmitOrder { symbol, order } => {
            exchange.execute(symbol, order, reports);
        }
        JournalEvent::CancelOrder { symbol, order_id } => {
            exchange.cancel(symbol, order_id, reports);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::types::*;

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
            order_type: OrderType::Limit { price: price(p) },
            time_in_force: TimeInForce::GTC,
            quantity: qty(q),
        }
    }

    #[test]
    fn replay_reproduces_identical_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replay.journal");

        // Build up some state.
        let mut reports = Vec::new();
        {
            let mut je = JournaledExchange::create(&path).unwrap();
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
            je.cancel(Symbol(1), OrderId(3), &mut reports).unwrap();
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
        let recovered = JournaledExchange::recover(&path).unwrap();
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
            let mut je = JournaledExchange::create(&path).unwrap();
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
        let mut reader = super::super::reader::JournalReader::open(&path).unwrap();
        let mut replay_exchange = Exchange::new();
        let mut replay_reports = Vec::new();

        while let Some(entry) = reader.next_entry().unwrap() {
            replay_event(&mut replay_exchange, &entry.event, &mut replay_reports);
        }

        assert_eq!(original_reports, replay_reports);
    }

    #[test]
    fn recover_continues_appending() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("continue.journal");

        {
            let mut je = JournaledExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
            assert_eq!(je.next_sequence(), 3);
        }

        // Recover and append more.
        {
            let mut je = JournaledExchange::recover(&path).unwrap();
            assert_eq!(je.next_sequence(), 3);

            je.deposit(ACCT_B, BTC, 500).unwrap();
            assert_eq!(je.next_sequence(), 4);
        }

        // Recover again — should see all 3 events.
        let je = JournaledExchange::recover(&path).unwrap();
        assert_eq!(je.next_sequence(), 4);
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
            let _je = JournaledExchange::create(&path).unwrap();
        }

        let je = JournaledExchange::recover(&path).unwrap();
        assert_eq!(je.next_sequence(), 1);
    }

    #[test]
    fn crash_mid_write_recovers_gracefully() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("crash.journal");

        {
            let mut je = JournaledExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
        }

        let original_len = std::fs::metadata(&path).unwrap().len();

        // Simulate crash by truncating last entry.
        {
            let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            file.set_len(original_len - 3).unwrap();
        }

        // Recovery should replay the first event (AddInstrument) but not the truncated Deposit.
        let je = JournaledExchange::recover(&path).unwrap();
        assert_eq!(je.next_sequence(), 2); // Only 1 event replayed.
        assert_eq!(je.exchange().accounts().balance(ACCT_A, USD).available, 0);

        // File should be truncated to remove the garbage trailing bytes.
        let recovered_len = std::fs::metadata(&path).unwrap().len();
        assert!(recovered_len < original_len - 3);
    }

    #[test]
    fn crash_recovery_then_append_no_garbage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no_garbage.journal");

        {
            let mut je = JournaledExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
        }

        // Simulate crash.
        {
            let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            let len = std::fs::metadata(&path).unwrap().len();
            file.set_len(len - 3).unwrap();
        }

        // Recover and append a new event.
        {
            let mut je = JournaledExchange::recover(&path).unwrap();
            je.deposit(ACCT_A, USD, 50_000).unwrap();
        }

        // Full re-recovery should see both events cleanly (no garbage between).
        let je = JournaledExchange::recover(&path).unwrap();
        assert_eq!(je.next_sequence(), 3);
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
            let mut je = JournaledExchange::create(&journal_path).unwrap();
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

            // Save snapshot at this point (seq=4).
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
        let je = JournaledExchange::recover_from_snapshot(&snap_path, &journal_path).unwrap();
        assert_eq!(je.next_sequence(), 6);
        // Buyer got 20 BTC from fill.
        assert_eq!(je.exchange().accounts().balance(ACCT_A, BTC).available, 20);
        // Seller still has 30 resting (50 - 20 filled).
        assert_eq!(je.exchange().accounts().balance(ACCT_B, BTC).reserved, 30);
    }
}
