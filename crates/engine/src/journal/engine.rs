//! Engine-side journal recovery tests (formerly contained the
//! `JournaledExchange` synchronous wrapper, now deleted in favour of
//! `melin_transport_core::JournaledApp<Exchange, _>`). The wrapper's
//! API survives only as a test harness inside the `tests` module so
//! the long-standing recovery / snapshot / crash tests don't have to
//! open-code the journal-then-apply dance.

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crate::journal::{BufferedWriter, SectorWriter};
    use crate::types::*;

    use std::path::Path;

    use melin_journal::{JournalError, JournalEvent, JournalWrite};
    use melin_transport_core::journaled_app::{JournaledApp, JournaledAppError};

    use crate::exchange::Exchange;
    use crate::trading_event::TradingEvent;
    use melin_transport_core::snapshot;

    /// Synchronous journal-then-apply harness. Wraps
    /// `JournaledApp<Exchange, BufferedWriter>` and re-exposes the
    /// per-event methods (`execute`, `deposit`, …) the recovery tests
    /// below need, so they read close to their pre-deletion shape.
    /// Production never journals-then-applies on the same thread; it
    /// runs the journal stage and matching stage on separate disruptor
    /// rings. This harness exists only because every test would
    /// otherwise open-code the same `writer.append` + `exchange.apply`
    /// dance.
    struct TestExchange {
        inner: JournaledApp<Exchange, BufferedWriter>,
    }

    impl TestExchange {
        fn create(path: &Path) -> Result<Self, JournaledAppError> {
            JournaledApp::create(Exchange::with_capacity(), path).map(|inner| Self { inner })
        }

        fn recover(path: &Path) -> Result<Self, JournaledAppError> {
            JournaledApp::recover(Exchange::with_capacity(), path).map(|inner| Self { inner })
        }

        fn recover_from_snapshot(
            snapshot_path: &Path,
            journal_path: &Path,
        ) -> Result<Self, JournaledAppError> {
            JournaledApp::recover_from_snapshot(snapshot_path, journal_path)
                .map(|inner| Self { inner })
        }

        fn from_parts(exchange: Exchange, writer: BufferedWriter) -> Self {
            Self {
                inner: JournaledApp::from_parts(exchange, writer),
            }
        }

        fn save_snapshot(&self, path: &Path) -> Result<(), JournaledAppError> {
            self.inner.save_snapshot(path)
        }

        fn rotate_segment(&mut self) -> Result<(), JournaledAppError> {
            self.inner.rotate_segment()
        }

        fn exchange(&self) -> &Exchange {
            self.inner.app()
        }

        fn next_sequence(&self) -> u64 {
            self.inner.next_sequence()
        }

        fn writer_chain_hash(&self) -> Option<[u8; 32]> {
            self.inner.chain_hash()
        }

        fn journal_size(&self) -> u64 {
            self.inner.journal_size()
        }

        fn add_instrument(&mut self, spec: InstrumentSpec) -> Result<(), JournalError> {
            let mut reports = Vec::new();
            self.inner
                .apply_journaled(TradingEvent::AddInstrument { spec }, &mut reports)?;
            Ok(())
        }

        fn deposit(
            &mut self,
            account: AccountId,
            currency: CurrencyId,
            amount: u64,
        ) -> Result<(), JournalError> {
            let mut reports = Vec::new();
            self.inner.apply_journaled(
                TradingEvent::Deposit {
                    account,
                    currency,
                    amount,
                },
                &mut reports,
            )?;
            Ok(())
        }

        fn cancel_all(
            &mut self,
            account: AccountId,
            reports: &mut Vec<ExecutionReport>,
        ) -> Result<(), JournalError> {
            self.inner
                .apply_journaled(TradingEvent::CancelAll { account }, reports)?;
            Ok(())
        }

        /// Journal the withdraw event unconditionally (so replay re-
        /// fires the same rejection), then call `Exchange::withdraw`
        /// directly to capture its rejection. The pipeline's
        /// `TradingEvent::Withdraw` arm discards this error today;
        /// `Exchange::withdraw` is the only API that surfaces it.
        fn withdraw(
            &mut self,
            account: AccountId,
            currency: CurrencyId,
            amount: u64,
        ) -> Result<(), RejectReason> {
            self.inner
                .writer_mut()
                .append(&JournalEvent::App(TradingEvent::Withdraw {
                    account,
                    currency,
                    amount,
                }))
                .expect("journal write");
            self.inner.app_mut().withdraw(account, currency, amount)
        }

        fn set_risk_limits(
            &mut self,
            symbol: Symbol,
            limits: RiskLimits,
        ) -> Result<(), JournalError> {
            let mut reports = Vec::new();
            self.inner
                .apply_journaled(TradingEvent::SetRiskLimits { symbol, limits }, &mut reports)?;
            Ok(())
        }

        fn set_circuit_breaker(
            &mut self,
            symbol: Symbol,
            config: CircuitBreakerConfig,
        ) -> Result<(), JournalError> {
            let mut reports = Vec::new();
            self.inner.apply_journaled(
                TradingEvent::SetCircuitBreaker { symbol, config },
                &mut reports,
            )?;
            Ok(())
        }

        fn set_fee_schedule(
            &mut self,
            symbol: Symbol,
            schedule: FeeSchedule,
            reports: &mut Vec<ExecutionReport>,
        ) -> Result<(), JournalError> {
            self.inner
                .apply_journaled(TradingEvent::SetFeeSchedule { symbol, schedule }, reports)?;
            Ok(())
        }

        fn execute(
            &mut self,
            symbol: Symbol,
            order: Order,
            reports: &mut Vec<ExecutionReport>,
        ) -> Result<(), JournalError> {
            self.inner
                .apply_journaled(TradingEvent::SubmitOrder { symbol, order }, reports)?;
            Ok(())
        }

        fn cancel(
            &mut self,
            symbol: Symbol,
            account: AccountId,
            order_id: OrderId,
            reports: &mut Vec<ExecutionReport>,
        ) -> Result<(), JournalError> {
            self.inner.apply_journaled(
                TradingEvent::CancelOrder {
                    symbol,
                    account,
                    order_id,
                },
                reports,
            )?;
            Ok(())
        }

        fn tick(
            &mut self,
            now_ns: u64,
            reports: &mut Vec<ExecutionReport>,
        ) -> Result<(), JournalError> {
            self.inner.tick_journaled(now_ns, reports)?;
            Ok(())
        }
    }

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

        // Replay should produce identical reports. Walk the journal
        // and feed each App event through the canonical
        // `Application::apply` dispatch — same path the production
        // matching stage uses during recovery.
        let mut reader = crate::journal::JournalReader::open(&path).unwrap();
        let mut replay_exchange = Exchange::new();
        let mut replay_reports = Vec::new();
        let mut last_drain_ns: u64 = 0;

        use melin_app::Application;
        while let Some(entry) = reader.next_entry().unwrap() {
            match entry.event {
                JournalEvent::App(event) => {
                    replay_exchange.check_request_seq(entry.key_hash, entry.request_seq);
                    if entry.timestamp_ns > last_drain_ns {
                        last_drain_ns = entry.timestamp_ns;
                        replay_exchange
                            .drain_due_scheduled_tasks(entry.timestamp_ns, &mut replay_reports);
                    }
                    let ctx = melin_app::ApplyCtx {
                        now_ns: entry.timestamp_ns,
                        journal_sequence: entry.sequence,
                        active_connections: 0,
                        events_processed: 0,
                        key_hash: entry.key_hash,
                    };
                    replay_exchange.apply(event, &ctx, &mut replay_reports);
                }
                JournalEvent::Tick { now_ns } => {
                    replay_exchange.drain_due_scheduled_tasks(now_ns, &mut replay_reports);
                }
                JournalEvent::GenesisHash { .. }
                | JournalEvent::Checkpoint { .. }
                | JournalEvent::Shutdown => {}
            }
        }

        assert_eq!(original_reports, replay_reports);
    }

    #[test]
    fn ticks_drive_gtd_expiry_through_replay() {
        use crate::journal::unix_epoch_nanos;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gtd_ticks.journal");

        // The matching/replay stage drains the scheduler at every event
        // using the journal entry's `timestamp_ns` (wall-clock at write
        // time). GTD `expiry_ns` therefore has to be in the same wall-clock
        // domain, comfortably in the future of every entry's timestamp,
        // for the test to control which Tick fires which order.
        let now = unix_epoch_nanos();
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
    fn save_snapshot_then_rotate_produces_valid_files() {
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
        engine.save_snapshot(&snap_path).unwrap();
        engine.rotate_segment().unwrap();

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
        engine.save_snapshot(&snap_path).unwrap();
        engine.rotate_segment().unwrap();

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
        engine.save_snapshot(&snap_path).unwrap();
        engine.rotate_segment().unwrap();
        assert!(std::path::Path::new(&format!("{}.000001", journal_path.display())).exists());

        engine.deposit(ACCT_A, BTC, 500).unwrap();
        engine.save_snapshot(&snap_path).unwrap();
        engine.rotate_segment().unwrap();
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

        engine.save_snapshot(&snap_path).unwrap();
        engine.rotate_segment().unwrap();

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
        let (_, _, loaded_hash) = snapshot::load::<Exchange>(&snap_path).unwrap();
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
        engine.save_snapshot(&snap_path).unwrap();
        engine.rotate_segment().unwrap();

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

            engine.save_snapshot(&snap_path).unwrap();
            engine.rotate_segment().unwrap();

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
        // Property test on `Exchange::withdraw` directly: it must
        // surface the underlying RejectReason rather than returning Ok
        // on failure.
        //
        // Note: in the live pipeline the `TradingEvent::Withdraw` arm
        // (see `application_impl.rs`) currently *discards* this error
        // and the client sees no response. A test routed through the
        // journaled path would be testing a synthetic contract; the
        // domain property lives on `Exchange::withdraw`.
        let mut exchange = Exchange::new();
        exchange.deposit(ACCT_A, USD, 100);
        let err = exchange.withdraw(ACCT_A, USD, 200).unwrap_err();
        assert_eq!(err, RejectReason::InsufficientBalance);
        // Balance unchanged.
        assert_eq!(exchange.accounts().balance(ACCT_A, USD).available, 100);
    }

    #[test]
    fn withdraw_with_resting_orders_returns_error() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        let mut reports = Vec::new();
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10),
            &mut reports,
        );
        let err = exchange.withdraw(ACCT_A, USD, 1).unwrap_err();
        assert_eq!(err, RejectReason::HasRestingOrders);
    }

    #[test]
    fn withdraw_unknown_account_returns_error() {
        let mut exchange = Exchange::new();
        let err = exchange.withdraw(ACCT_A, USD, 1).unwrap_err();
        assert_eq!(err, RejectReason::UnknownAccount);
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
            assert_eq!(err, RejectReason::HasRestingOrders);
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
            je.save_snapshot(&snap_path).unwrap();
            je.rotate_segment().unwrap();

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
            let ts = crate::journal::unix_epoch_nanos();
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
            let ts = crate::journal::unix_epoch_nanos();
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
        let (restored, _seq, _chain) = snapshot::load::<Exchange>(&snapshot_path).unwrap();
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
    /// Shrinks the journal prealloc chunk to 1 MiB for this test so the
    /// per-iteration `fallocate` doesn't dominate wall time, and runs the
    /// truncation grid across all available cores — each iteration is
    /// independent (its own work file, its own truncation point), and the
    /// per-iteration cost is overwhelmingly kernel I/O wait (fallocate +
    /// fdatasync), so the workers add negligible CPU pressure for
    /// neighbouring tests.
    #[test]
    fn crash_at_every_byte_offset_recovers() {
        // Shrink the prealloc chunk so each `recover()` reopen-for-append
        // doesn't pay the 256 MiB fallocate cost. Persists for the rest
        // of the process, but only matters here.
        melin_journal::test_utils::set_prealloc_chunk_bytes_override(Some(1024 * 1024));

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

        // Shrink the original to its valid data size. The pre-allocated
        // tail (1 MiB under the override above) would otherwise be
        // copied per iteration.
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&original)
                .unwrap();
            f.set_len(end).unwrap();
        }

        // Parallelize across worker threads. Each iteration is
        // independent (its own truncation point, its own work file), so
        // the loop scales near-linearly with cores. Per-iteration cost
        // is dominated by kernel I/O wait (small `fallocate` + the
        // post-recovery `fdatasync`), so the workers add negligible CPU
        // pressure for neighbouring tests under nextest.
        let num_threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let work_dir = dir.path().to_path_buf();
        std::thread::scope(|scope| {
            for tid in 0..num_threads {
                let original = &original;
                let work_dir = &work_dir;
                scope.spawn(move || {
                    // Per-thread work file so concurrent recoveries
                    // don't trample each other's truncated journal.
                    let work = work_dir.join(format!("work-{tid}.journal"));
                    // Stride the offset range across threads so each
                    // worker sees a mix of small and large truncations
                    // — keeps per-thread workload balanced even when
                    // recovery cost varies with truncation point.
                    let mut trunc_at = header_end + tid as u64;
                    while trunc_at <= end {
                        copy_file(original, &work);
                        {
                            let f = std::fs::OpenOptions::new().write(true).open(&work).unwrap();
                            f.set_len(trunc_at).unwrap();
                        }

                        let mut je = TestExchange::recover(&work).unwrap();
                        assert!(je.next_sequence() >= 1, "seq underflow at byte {trunc_at}");

                        je.deposit(ACCT_A, USD, 1).unwrap();

                        drop(je);
                        let je2 = TestExchange::recover(&work).unwrap();
                        assert!(
                            je2.next_sequence() >= 2,
                            "double-recovery seq too low at byte {trunc_at}"
                        );

                        trunc_at += num_threads as u64;
                    }
                });
            }
        });
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
        engine.save_snapshot(&snap_path).unwrap();
        engine.rotate_segment().unwrap();

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
        let (snap_exchange, snap_seq, _snap_hash) = snapshot::load::<Exchange>(&snap_path).unwrap();
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
        let (exchange, seq, chain_hash) = snapshot::load::<Exchange>(&snap_path).unwrap();
        let writer = BufferedWriter::create_continuing(&journal_path, seq + 1, chain_hash).unwrap();
        let je = TestExchange::from_parts(exchange, writer);
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
        engine.save_snapshot(&snap_path).unwrap();
        engine.rotate_segment().unwrap();

        // Rotation 2.
        engine
            .execute(
                Symbol(1),
                limit_order(2, ACCT_A, Side::Buy, 100, 20),
                &mut reports,
            )
            .unwrap();
        engine.deposit(ACCT_A, BTC, 500).unwrap();
        engine.save_snapshot(&snap_path).unwrap();
        engine.rotate_segment().unwrap();

        // Rotation 3.
        engine.deposit(ACCT_B, USD, 999).unwrap();
        engine
            .execute(
                Symbol(1),
                limit_order(3, ACCT_A, Side::Buy, 100, 10),
                &mut reports,
            )
            .unwrap();
        engine.save_snapshot(&snap_path).unwrap();
        engine.rotate_segment().unwrap();

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

    /// Apply a TestEvent to the `TestExchange` harness.
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
                // (insufficient balance, resting orders); both Ok and
                // Err are expected here, the journaled event captures
                // either outcome for deterministic replay.
                let _ = je.withdraw(*acct, *cur, *amt);
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
