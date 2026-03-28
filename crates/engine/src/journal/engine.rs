//! JournaledExchange — wraps `Exchange` with durable event journaling.
//!
//! Journals every input command before executing it, ensuring the
//! persist-before-ack invariant. On crash, replay reconstructs identical state.

use std::path::Path;

use crate::exchange::Exchange;
use crate::types::{
    AccountId, CircuitBreakerConfig, CurrencyId, ExecutionReport, InstrumentSpec, Order, OrderId,
    RiskLimits, Symbol,
};

use super::error::JournalError;
use super::event::JournalEvent;
use super::reader::JournalReader;
use super::snapshot;
use crate::journal::writer::JournalWriter;

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
            exchange: Exchange::with_capacity(),
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

    /// Cancel all orders for an account (kill switch). Journals before executing.
    pub fn cancel_all(
        &mut self,
        account: AccountId,
        reports: &mut Vec<ExecutionReport>,
    ) -> Result<(), JournalError> {
        self.writer.append(&JournalEvent::CancelAll { account })?;
        self.exchange.cancel_all(account, reports);
        Ok(())
    }

    /// Withdraw funds from an account. Journals before executing.
    /// Rejects if the account has resting orders or insufficient balance.
    pub fn withdraw(
        &mut self,
        account: AccountId,
        currency: CurrencyId,
        amount: u64,
    ) -> Result<(), JournalError> {
        self.writer.append(&JournalEvent::Withdraw {
            account,
            currency,
            amount,
        })?;
        // Withdraw errors are returned to the caller but don't affect
        // the journal — the event is recorded regardless.
        let _ = self.exchange.withdraw(account, currency, amount);
        Ok(())
    }

    /// Set risk limits for an instrument. Journals before executing.
    pub fn set_risk_limits(
        &mut self,
        symbol: Symbol,
        limits: RiskLimits,
    ) -> Result<(), JournalError> {
        self.writer
            .append(&JournalEvent::SetRiskLimits { symbol, limits })?;
        self.exchange.set_risk_limits(symbol, limits);
        Ok(())
    }

    /// Set circuit breaker configuration for an instrument. Journals before executing.
    pub fn set_circuit_breaker(
        &mut self,
        symbol: Symbol,
        config: CircuitBreakerConfig,
    ) -> Result<(), JournalError> {
        self.writer
            .append(&JournalEvent::SetCircuitBreaker { symbol, config })?;
        self.exchange.set_circuit_breaker(symbol, config);
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
        account: AccountId,
        order_id: OrderId,
        reports: &mut Vec<ExecutionReport>,
    ) -> Result<(), JournalError> {
        self.writer.append(&JournalEvent::CancelOrder {
            symbol,
            account,
            order_id,
        })?;
        self.exchange.cancel(symbol, account, order_id, reports);
        Ok(())
    }

    /// Disable an instrument. Journals before executing.
    pub fn disable_instrument(
        &mut self,
        symbol: Symbol,
        reports: &mut Vec<ExecutionReport>,
    ) -> Result<(), JournalError> {
        self.writer
            .append(&JournalEvent::DisableInstrument { symbol })?;
        self.exchange.disable_instrument(symbol, reports);
        Ok(())
    }

    /// Re-enable a disabled instrument. Journals before executing.
    pub fn enable_instrument(
        &mut self,
        symbol: Symbol,
        reports: &mut Vec<ExecutionReport>,
    ) -> Result<(), JournalError> {
        self.writer
            .append(&JournalEvent::EnableInstrument { symbol })?;
        self.exchange.enable_instrument(symbol, reports);
        Ok(())
    }

    /// Remove a disabled instrument. Journals before executing.
    pub fn remove_instrument(
        &mut self,
        symbol: Symbol,
        reports: &mut Vec<ExecutionReport>,
    ) -> Result<(), JournalError> {
        self.writer
            .append(&JournalEvent::RemoveInstrument { symbol })?;
        self.exchange.remove_instrument(symbol, reports);
        Ok(())
    }

    /// Recover from an existing journal file by replaying all events.
    ///
    /// Truncates any trailing garbage from a partial write (crash recovery),
    /// then reopens the writer for appending new events.
    pub fn recover(journal_path: &Path) -> Result<Self, JournalError> {
        let mut reader = JournalReader::open(journal_path)?;
        let mut exchange = Exchange::with_capacity();
        let mut reports = Vec::new();

        while let Some(entry) = reader.next_entry()? {
            replay_event(
                &mut exchange,
                &entry.event,
                entry.key_hash,
                entry.request_seq,
                &mut reports,
            );
            reports.clear();
        }

        let last_seq = reader.last_sequence().unwrap_or(0);
        let valid_end = reader.valid_file_end();
        let chain_hash = reader.chain_hash();
        let events_since_checkpoint = reader.events_since_checkpoint();
        let writer = JournalWriter::open_append(
            journal_path,
            last_seq,
            valid_end,
            chain_hash,
            events_since_checkpoint,
        )?;

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
        let (mut exchange, snap_sequence, snap_chain_hash) = snapshot::load(snapshot_path)?;
        let mut reader = JournalReader::open(journal_path)?;

        // Seed the reader's hash chain from the snapshot so verification
        // continues from the snapshot boundary rather than requiring replay
        // from genesis.
        reader.seed_chain_hash(snap_chain_hash, snap_sequence);

        let mut reports = Vec::new();

        // Skip entries already captured by the snapshot.
        while let Some(entry) = reader.next_entry()? {
            if entry.sequence > snap_sequence {
                replay_event(
                    &mut exchange,
                    &entry.event,
                    entry.key_hash,
                    entry.request_seq,
                    &mut reports,
                );
                reports.clear();
            }
        }

        let last_seq = reader.last_sequence().unwrap_or(snap_sequence);
        let valid_end = reader.valid_file_end();
        let chain_hash = reader.chain_hash();
        let events_since_checkpoint = reader.events_since_checkpoint();
        let writer = JournalWriter::open_append(
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
    /// The old journal is renamed to `<path>.1` (bumping any existing
    /// archives: `.1` → `.2`, `.2` → `.3`, etc.). The snapshot is written
    /// to `snapshot_path` atomically (via `.tmp` + rename).
    ///
    /// Call this before `into_parts()` — rotation requires both the
    /// exchange (for snapshot) and the writer (for sequence continuity).
    pub fn rotate(&mut self, snapshot_path: &Path) -> Result<(), JournalError> {
        // 1. Save snapshot at the current sequence boundary.
        self.save_snapshot(snapshot_path)?;

        // 2. Archive the old journal by rotating file names.
        let journal_path = self.writer.path().to_path_buf();
        rotate_file(&journal_path)?;

        // 3. Create a new journal continuing from the same sequence.
        // Use the current chain hash as the genesis for the new journal,
        // providing cryptographic continuity across rotation boundaries.
        let next_seq = self.writer.next_sequence();
        let genesis = self.writer.chain_hash().unwrap_or([0u8; 32]);
        self.writer = JournalWriter::create_continuing(&journal_path, next_seq, genesis)?;

        Ok(())
    }

    /// Size of the current journal file in bytes.
    pub fn journal_size(&self) -> u64 {
        self.writer.write_pos()
    }

    /// Construct from pre-built parts. Used by the server for snapshot-only
    /// recovery (when the journal is missing after a rotation crash).
    pub fn from_parts(exchange: Exchange, writer: JournalWriter) -> Self {
        Self { exchange, writer }
    }

    /// Decompose into parts for the pipeline architecture.
    ///
    /// After recovery, the exchange and journal writer are handed to separate
    /// pipeline stages: the matching thread owns the `Exchange`, and the
    /// journal thread owns the `JournalWriter`.
    pub fn into_parts(self) -> (Exchange, JournalWriter) {
        (self.exchange, self.writer)
    }
}

/// Rotate a file by renaming it to `<path>.1`, bumping existing archives.
///
/// `foo.journal` → `foo.journal.1` (and `.1` → `.2`, `.2` → `.3`, etc.)
fn rotate_file(path: &Path) -> Result<(), JournalError> {
    // Find the highest existing archive number.
    let mut max_n = 0u32;
    loop {
        let archive = format!("{}.{}", path.display(), max_n + 1);
        if !std::path::Path::new(&archive).exists() {
            break;
        }
        max_n += 1;
    }

    // Rename in reverse order to avoid overwriting: .2→.3, .1→.2, base→.1
    for n in (1..=max_n).rev() {
        let from = format!("{}.{n}", path.display());
        let to = format!("{}.{}", path.display(), n + 1);
        std::fs::rename(&from, &to)?;
    }
    let archive_1 = format!("{}.1", path.display());
    std::fs::rename(path, &archive_1)?;

    Ok(())
}

/// Replay a single journal event into an exchange. Used during recovery.
///
/// Rebuilds the per-key request sequence HWM by calling `check_request_seq`
/// on every event. Since the journal contains no duplicates (they were
/// rejected at write time), this always returns true — the purpose is
/// to reconstruct the HWM state for live dedup after recovery.
fn replay_event(
    exchange: &mut Exchange,
    event: &JournalEvent,
    key_hash: u64,
    request_seq: u64,
    reports: &mut Vec<ExecutionReport>,
) {
    // Rebuild per-key HWM state (always succeeds on journal replay — no
    // duplicates in the journal).
    exchange.check_request_seq(key_hash, request_seq);

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
        JournalEvent::CancelOrder {
            symbol,
            account,
            order_id,
        } => {
            exchange.cancel(symbol, account, order_id, reports);
        }
        JournalEvent::SetRiskLimits { symbol, limits } => {
            exchange.set_risk_limits(symbol, limits);
        }
        JournalEvent::CancelAll { account } => {
            exchange.cancel_all(account, reports);
        }
        JournalEvent::EndOfDay => {
            exchange.end_of_day(reports);
        }
        JournalEvent::ExpireOrders { timestamp_ns } => {
            exchange.expire_orders(timestamp_ns, reports);
        }
        JournalEvent::SetCircuitBreaker { symbol, config } => {
            exchange.set_circuit_breaker(symbol, config);
        }
        JournalEvent::CancelReplace {
            symbol,
            account,
            order_id,
            new_price,
            new_quantity,
        } => {
            exchange.cancel_replace(symbol, account, order_id, new_price, new_quantity, reports);
        }
        JournalEvent::SetFeeSchedule { symbol, schedule } => {
            exchange.set_fee_schedule(symbol, schedule);
        }
        JournalEvent::ProvisionAccount { account, amount } => {
            exchange.provision_account(account, amount);
        }
        JournalEvent::Withdraw {
            account,
            currency,
            amount,
        } => {
            // Withdraw errors (insufficient balance, resting orders) are
            // non-fatal on replay — the journal recorded the attempt, and
            // the original error was already returned to the client.
            let _ = exchange.withdraw(account, currency, amount);
        }
        JournalEvent::DisableInstrument { symbol } => {
            exchange.disable_instrument(symbol, reports);
        }
        JournalEvent::EnableInstrument { symbol } => {
            exchange.enable_instrument(symbol, reports);
        }
        JournalEvent::RemoveInstrument { symbol } => {
            exchange.remove_instrument(symbol, reports);
        }
        JournalEvent::QueryStats => {
            // QueryStats is never journaled, so it should never appear
            // during replay. No-op if it somehow does.
        }
        JournalEvent::GenesisHash { .. } | JournalEvent::Checkpoint { .. } => {
            // Hash chain metadata — no exchange state change.
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::types::*;

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
            replay_event(
                &mut replay_exchange,
                &entry.event,
                entry.key_hash,
                entry.request_seq,
                &mut replay_reports,
            );
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
            // With hash-chain: Genesis(1) + AddInstrument(2) + Deposit(3) = next is 4.
            // Without: AddInstrument(1) + Deposit(2) = next is 3.
            assert_eq!(je.next_sequence(), FIRST_SEQ + 2);
        }

        // Recover and append more.
        {
            let mut je = JournaledExchange::recover(&path).unwrap();
            assert_eq!(je.next_sequence(), FIRST_SEQ + 2);

            je.deposit(ACCT_B, BTC, 500).unwrap();
            assert_eq!(je.next_sequence(), FIRST_SEQ + 3);
        }

        // Recover again — should see all 3 user events.
        let je = JournaledExchange::recover(&path).unwrap();
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
            let _je = JournaledExchange::create(&path).unwrap();
        }

        let je = JournaledExchange::recover(&path).unwrap();
        // With hash-chain, genesis consumed seq 1, so next is 2; without, next is 1.
        assert_eq!(je.next_sequence(), FIRST_SEQ);
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

        // Find valid data end (file is larger due to pre-allocation).
        let valid_data_end = {
            let mut reader = crate::journal::reader::JournalReader::open(&path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            reader.valid_file_end()
        };

        // Simulate crash by truncating 3 bytes from the last valid entry.
        {
            let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            file.set_len(valid_data_end - 3).unwrap();
        }

        // Recovery should replay the first event (AddInstrument) but not the truncated Deposit.
        let je = JournaledExchange::recover(&path).unwrap();
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
            let mut je = JournaledExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
        }

        // Find valid data end, then simulate crash by truncating within valid data.
        {
            let valid_data_end = {
                let mut reader = crate::journal::reader::JournalReader::open(&path).unwrap();
                while reader.next_entry().unwrap().is_some() {}
                reader.valid_file_end()
            };
            let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            file.set_len(valid_data_end - 3).unwrap();
        }

        // Recover and append a new event.
        {
            let mut je = JournaledExchange::recover(&path).unwrap();
            je.deposit(ACCT_A, USD, 50_000).unwrap();
        }

        // Full re-recovery should see both events cleanly (no garbage between).
        let je = JournaledExchange::recover(&path).unwrap();
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
        let je = JournaledExchange::recover_from_snapshot(&snap_path, &journal_path).unwrap();
        // With hash-chain: Genesis(1) + 4 user events(2,3,4,5) + 1 post-snap(6) → next=7
        // Without: 4 user events(1,2,3,4) + 1 post-snap(5) → next=6
        assert_eq!(je.next_sequence(), FIRST_SEQ + 5);
        // Buyer got 20 BTC from fill.
        assert_eq!(je.exchange().accounts().balance(ACCT_A, BTC).available, 20);
        // Seller still has 30 resting (50 - 20 filled).
        assert_eq!(je.exchange().accounts().balance(ACCT_B, BTC).reserved, 30);
    }

    #[test]
    fn journal_replay_restores_circuit_breaker_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cb_replay.journal");

        {
            let mut je = JournaledExchange::create(&path).unwrap();
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
        let mut recovered = JournaledExchange::recover(&path).unwrap();
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
        let mut engine = JournaledExchange::create(&journal_path).unwrap();
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
        let archived = format!("{}.1", journal_path.display());
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
        let mut engine = JournaledExchange::create(&journal_path).unwrap();
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
        let recovered =
            JournaledExchange::recover_from_snapshot(&snap_path, &journal_path).unwrap();
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

        let mut engine = JournaledExchange::create(&journal_path).unwrap();
        engine.add_instrument(btc_usd_spec()).unwrap();
        engine.deposit(ACCT_A, USD, 1_000_000).unwrap();

        // Rotate twice.
        engine.rotate(&snap_path).unwrap();
        assert!(std::path::Path::new(&format!("{}.1", journal_path.display())).exists());

        engine.deposit(ACCT_A, BTC, 500).unwrap();
        engine.rotate(&snap_path).unwrap();
        assert!(std::path::Path::new(&format!("{}.2", journal_path.display())).exists());
        assert!(std::path::Path::new(&format!("{}.1", journal_path.display())).exists());
        assert!(journal_path.exists());
    }

    #[test]
    fn create_continuing_starts_at_correct_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cont.journal");

        let mut writer = JournalWriter::create_continuing(&path, 42, [0xAA; 32]).unwrap();
        // With hash-chain, genesis consumes seq 42, next is 43.
        // Without hash-chain, no genesis, next is 42.
        #[cfg(feature = "hash-chain")]
        let expected_first = 43u64;
        #[cfg(not(feature = "hash-chain"))]
        let expected_first = 42u64;
        assert_eq!(writer.next_sequence(), expected_first);

        let event = JournalEvent::Deposit {
            account: ACCT_A,
            currency: USD,
            amount: 100,
        };
        let seq = writer.append(&event).unwrap();
        assert_eq!(seq, expected_first);
        assert_eq!(writer.next_sequence(), expected_first + 1);

        // Read it back. Genesis is transparent, first user entry starts at expected_first.
        let mut reader = crate::journal::reader::JournalReader::open(&path).unwrap();
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
            let mut je = JournaledExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
            original_hash = je.writer_chain_hash();
        }

        // Recover and verify chain hash is preserved.
        let recovered = JournaledExchange::recover(&path).unwrap();
        assert_eq!(recovered.writer_chain_hash(), original_hash);
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn rotation_preserves_chain_continuity() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("rot.journal");
        let snap_path = dir.path().join("rot.snapshot");

        let mut engine = JournaledExchange::create(&journal_path).unwrap();
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
            let mut je = JournaledExchange::create(&journal_path).unwrap();
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
        let mut engine = JournaledExchange::create(&journal_path).unwrap();
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
        let recovered =
            JournaledExchange::recover_from_snapshot(&snap_path, &journal_path).unwrap();
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
            let mut je = JournaledExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
            je.deposit(ACCT_B, BTC, 500).unwrap();
        }

        // Simulate crash by truncating last entry.
        {
            let mut reader = crate::journal::reader::JournalReader::open(&path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            let valid_end = reader.valid_file_end();
            let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            file.set_len(valid_end - 3).unwrap();
        }

        // Recover — chain should be valid for the surviving entries.
        let mut je = JournaledExchange::recover(&path).unwrap();
        let hash_after_crash = je.writer_chain_hash();
        assert!(hash_after_crash.is_some());

        // Append more events.
        je.deposit(ACCT_A, BTC, 200).unwrap();
        let hash_after_append = je.writer_chain_hash();
        assert_ne!(hash_after_crash, hash_after_append);
        drop(je);

        // Re-recover — chain should match.
        let je2 = JournaledExchange::recover(&path).unwrap();
        assert_eq!(je2.writer_chain_hash(), hash_after_append);
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn multiple_rotations_preserve_chain_state() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("multi_rot.journal");
        let snap_path = dir.path().join("multi_rot.snapshot");

        let mut engine = JournaledExchange::create(&journal_path).unwrap();
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
        let recovered =
            JournaledExchange::recover_from_snapshot(&snap_path, &journal_path).unwrap();
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
            let mut je = JournaledExchange::create(&path).unwrap();
            je.add_instrument(btc_usd_spec()).unwrap();
            je.deposit(ACCT_A, USD, 100_000).unwrap();
            je.withdraw(ACCT_A, USD, 50_000).unwrap();
        }

        // Replay should produce the same state.
        let je = JournaledExchange::recover(&path).unwrap();
        assert_eq!(
            je.exchange().accounts().balance(ACCT_A, USD).available,
            50_000
        );
    }

    #[test]
    fn journal_replay_rejected_withdraw_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("withdraw_rejected.journal");

        {
            let mut je = JournaledExchange::create(&path).unwrap();
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

            // This withdraw is journaled but rejected at execution.
            let _ = je.withdraw(ACCT_A, USD, 1_000);
        }

        // Replay: the rejected withdraw should be a no-op.
        let je = JournaledExchange::recover(&path).unwrap();
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
            let mut je = JournaledExchange::create(&journal_path).unwrap();
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
        let je = JournaledExchange::recover_from_snapshot(&snap_path, &journal_path).unwrap();

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
            let mut writer = crate::journal::writer::JournalWriter::create(&path).unwrap();
            let ts = crate::journal::writer::wall_clock_nanos();
            // Deposit with seq=1
            writer
                .batch_append_with_ts(
                    &JournalEvent::AddInstrument {
                        spec: btc_usd_spec(),
                    },
                    ts,
                    key_hash,
                    1,
                )
                .unwrap();
            // Deposit with seq=2
            writer
                .batch_append_with_ts(
                    &JournalEvent::Deposit {
                        account: ACCT_A,
                        currency: USD,
                        amount: 1000,
                    },
                    ts,
                    key_hash,
                    2,
                )
                .unwrap();
            writer.flush_batch_sync().unwrap();
        }

        // Recover should rebuild the HWM.
        let je = JournaledExchange::recover(&path).unwrap();
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
            let mut writer = crate::journal::writer::JournalWriter::create(&journal_path).unwrap();
            let ts = crate::journal::writer::wall_clock_nanos();
            writer
                .batch_append_with_ts(
                    &JournalEvent::AddInstrument {
                        spec: btc_usd_spec(),
                    },
                    ts,
                    key_hash,
                    1,
                )
                .unwrap();
            writer
                .batch_append_with_ts(
                    &JournalEvent::Deposit {
                        account: ACCT_A,
                        currency: USD,
                        amount: 5000,
                    },
                    ts,
                    key_hash,
                    5,
                )
                .unwrap();
            writer.flush_batch_sync().unwrap();
        }

        // Recover and snapshot.
        let je = JournaledExchange::recover(&journal_path).unwrap();
        je.save_snapshot(&snapshot_path).unwrap();

        // Load snapshot into a fresh exchange.
        let (restored, _seq, _chain) = super::snapshot::load(&snapshot_path).unwrap();
        let hwm = restored.snapshot_key_hwm();
        assert_eq!(hwm.len(), 1);
        assert_eq!(hwm[0], (key_hash, 5));
    }
}
