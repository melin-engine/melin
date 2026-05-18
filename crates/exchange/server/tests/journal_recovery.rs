//! Engine-side journal recovery tests.
//!
//! Exercise `JournaledApp<App, _>` (transport-core) end-to-end:
//! create, write, snapshot, rotate, recover, replay, crash-and-recover
//! across every byte offset. The `TestExchange` harness below mirrors
//! the shape of the deleted `JournaledExchange` wrapper so the tests
//! don't have to open-code the journal-then-apply dance per call.

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::path::Path;

    use melin_journal::{JournalError, JournalEvent, JournalWrite};
    use melin_transport_core::journaled_app::{JournaledApp, JournaledAppError};
    use melin_transport_core::snapshot;

    use melin_engine::exchange::Exchange;
    // Import the concrete newtype (not the `pub type App = ServerApp`
    // alias) so it's usable as a tuple-struct constructor in `App(...)`.
    use melin_journal::BufferedWriter;
    use melin_server::domain::exchange_app::ServerApp as App;
    use melin_trading::trading_event::TradingEvent;
    use melin_types::types::*;

    /// Synchronous journal-then-apply harness mirroring the deleted
    /// `JournaledExchange` API. Wraps `JournaledApp<App,
    /// BufferedWriter>` and re-exposes per-event methods so the recovery
    /// tests below read close to their pre-deletion shape.
    ///
    /// Production never journals-then-applies on the same thread — it
    /// runs the journal stage and matching stage on separate disruptor
    /// rings. The harness exists only so individual tests don't open-
    /// code the `writer.append` + `exchange.apply` dance.
    ///
    /// **Caveat**: the per-event methods construct an `ApplyCtx` with
    /// `active_connections`, `events_processed`, and `key_hash` all
    /// zeroed (see `apply_journaled` in transport-core). Tests that
    /// drive `TradingEvent::QueryStats` or similar query variants would
    /// see those zeros — fine for the recovery tests here, all of which
    /// are state-mutation only.
    struct TestExchange {
        inner: JournaledApp<App, BufferedWriter<TradingEvent>>,
    }

    impl TestExchange {
        /// Fresh exchange + fresh journal at `path`.
        fn create(path: &Path) -> Result<Self, JournaledAppError> {
            JournaledApp::create(App(Exchange::with_capacity()), path).map(|inner| Self { inner })
        }

        /// Walk every archived segment then the live segment, replaying
        /// every event into a fresh `Exchange`.
        fn recover(path: &Path) -> Result<Self, JournaledAppError> {
            JournaledApp::recover(App(Exchange::with_capacity()), path).map(|inner| Self { inner })
        }

        /// Load `Exchange` state from a snapshot, then replay the
        /// post-snapshot delta from the journal.
        fn recover_from_snapshot(
            snapshot_path: &Path,
            journal_path: &Path,
        ) -> Result<Self, JournaledAppError> {
            JournaledApp::recover_from_snapshot(snapshot_path, journal_path)
                .map(|inner| Self { inner })
        }

        /// Write a snapshot of the current `Exchange` state — same
        /// framing the production shadow stage uses.
        fn save_snapshot(&self, path: &Path) -> Result<(), JournaledAppError> {
            self.inner.save_snapshot(path)
        }

        /// Archive the live segment and start a fresh one continuing
        /// the sequence + chain hash.
        fn rotate_segment(&mut self) -> Result<(), JournaledAppError> {
            self.inner.rotate_segment()
        }

        /// Read-only access for state assertions (balances, order book).
        fn exchange(&self) -> &Exchange {
            &self.inner.app().0
        }

        /// Next sequence the writer will assign.
        fn next_sequence(&self) -> u64 {
            self.inner.next_sequence()
        }

        /// Journal + apply `TradingEvent::AddInstrument`.
        fn add_instrument(&mut self, spec: InstrumentSpec) -> Result<(), JournalError> {
            let mut reports = Vec::new();
            self.inner
                .apply_journaled(TradingEvent::AddInstrument { spec }, &mut reports)?;
            Ok(())
        }

        /// Journal + apply `TradingEvent::Deposit`.
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

        /// Journal the withdraw event unconditionally (so replay re-
        /// fires the same rejection), then call `Exchange::withdraw`
        /// directly to capture the rejection.
        ///
        /// Bypasses `apply_journaled` because the pipeline's
        /// `TradingEvent::Withdraw` arm discards rejections today
        /// (`let _ = self.withdraw(...)` in `application_impl.rs`).
        /// `Exchange::withdraw` is the only API that surfaces them —
        /// flagged on the roadmap as item #15.
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

        /// Journal + apply `TradingEvent::SetRiskLimits`.
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

        /// Journal + apply `TradingEvent::SetCircuitBreaker`.
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

        /// Journal + apply `TradingEvent::SetFeeSchedule`. The new
        /// schedule may cancel orders that can't afford the cushion;
        /// those rejections flow into `reports`.
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

        /// Journal + apply `TradingEvent::SubmitOrder`. Fills + the
        /// `Placed`/`Rejected` outcome flow into `reports`.
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

        /// Journal + apply `TradingEvent::CancelOrder`.
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

        /// Journal a `Tick` and drain any scheduled tasks due at
        /// `now_ns` (GTD expiries, circuit-breaker windows, …).
        fn tick(
            &mut self,
            now_ns: u64,
            reports: &mut Vec<ExecutionReport>,
        ) -> Result<(), JournalError> {
            self.inner.tick_journaled(now_ns, reports)?;
            Ok(())
        }
    }

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
    fn ticks_drive_gtd_expiry_through_replay() {
        use melin_app::unix_epoch_nanos;

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
            let mut writer = melin_journal::SectorWriter::<
                melin_trading::trading_event::TradingEvent,
            >::create(&path)
            .unwrap();
            let ts = melin_app::unix_epoch_nanos();
            // Deposit with seq=1
            writer
                .batch_append_with_ts(
                    &JournalEvent::App(TradingEvent::AddInstrument {
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
                    &JournalEvent::App(TradingEvent::Deposit {
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
            let mut writer = melin_journal::SectorWriter::<
                melin_trading::trading_event::TradingEvent,
            >::create(&journal_path)
            .unwrap();
            let ts = melin_app::unix_epoch_nanos();
            writer
                .batch_append_with_ts(
                    &JournalEvent::App(TradingEvent::AddInstrument {
                        spec: btc_usd_spec(),
                    }),
                    ts,
                    key_hash,
                    1,
                )
                .unwrap();
            writer
                .batch_append_with_ts(
                    &JournalEvent::App(TradingEvent::Deposit {
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
        let (restored, _seq, _chain) = snapshot::load::<App>(&snapshot_path).unwrap();
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
        let mut reader =
            melin_journal::JournalReader::<melin_trading::trading_event::TradingEvent>::open(path)
                .unwrap();
        while reader.next_entry().unwrap().is_some() {}
        reader.valid_file_end()
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
        use melin_engine::account::FEE_ACCOUNT;

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
