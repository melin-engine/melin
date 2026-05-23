//! Trading-side [`AppFactory`] implementation.
//!
//! Owns the trading-domain construction recipe: empty / pre-sized
//! exchange, SEC-03/SEC-04 operator policy, and the bulk-seed
//! `AddInstrument` / `ProvisionAccount` events. Moves all four out
//! of `runtime/server.rs` so the runtime never references trading
//! event variants by name.

use melin_app::app_factory::AppFactory;
use melin_trading::trading_event::TradingEvent;
use melin_types::types::{AccountId, CurrencyId, InstrumentSpec, Symbol};

use crate::exchange_app::ServerApp;

/// Construction config for [`Factory`]. Mirrors the
/// trading-shaped fields of `ServerConfig`; kept as its own struct
/// so the binary can build one independently of the larger runtime
/// config when the eventual `ServerConfig` split happens.
#[derive(Debug, Clone, Copy)]
pub struct FactoryConfig {
    /// Number of accounts to provision at startup.
    pub accounts: u32,
    /// Number of instruments to register at startup.
    pub instruments: u32,
    /// SEC-03: maximum simultaneously open orders per account.
    pub max_orders_per_account: u32,
    /// SEC-04: token-bucket refill rate, orders per second. `0`
    /// disables the limiter.
    pub max_orders_per_second: u32,
    /// SEC-04: token-bucket capacity (max burst). `0` disables
    /// the limiter.
    pub max_orders_burst: u32,
}

/// Trading-side [`AppFactory`] producing `ServerApp` instances.
#[derive(Debug, Clone, Copy)]
pub struct Factory {
    config: FactoryConfig,
}

impl Factory {
    pub fn new(config: FactoryConfig) -> Self {
        Self { config }
    }
}

impl AppFactory for Factory {
    type App = ServerApp;

    fn empty(&self) -> ServerApp {
        ServerApp(melin_exchange_core::exchange::Exchange::with_capacity())
    }

    fn empty_for_seed(&self) -> ServerApp {
        let mut app = ServerApp(melin_exchange_core::exchange::Exchange::with_seed_capacity(
            self.config.accounts as usize,
            self.config.instruments as usize,
        ));
        self.apply_operator_policy(&mut app);
        app
    }

    fn apply_operator_policy(&self, app: &mut ServerApp) {
        // SEC-04 mismatch detection (must run BEFORE we apply the
        // new config). Non-empty bucket map paired with a disabled
        // limiter means we just restored a snapshot whose primary
        // had the limiter active, but the local operator forgot to
        // wire matching `--max-orders-per-second` / `--max-orders-burst`
        // flags. The engine will continue accepting all orders
        // unthrottled — silent until the replica is promoted and
        // starts diverging from the primary's accept/reject
        // decisions. Surface the misconfig loudly so operators catch
        // it at startup, not on incident.
        let restored_buckets = app.order_bucket_count();
        let limiter_disabled =
            self.config.max_orders_per_second == 0 || self.config.max_orders_burst == 0;
        if limiter_disabled && restored_buckets > 0 {
            tracing::warn!(
                restored_buckets,
                max_orders_per_second = self.config.max_orders_per_second,
                max_orders_burst = self.config.max_orders_burst,
                "config mismatch: snapshot carries rate-limit buckets but local limiter \
                 is disabled — primary and replica must run with matching values"
            );
        }

        app.set_max_open_orders_per_account(self.config.max_orders_per_account);
        app.set_max_orders_per_second(
            self.config.max_orders_per_second,
            self.config.max_orders_burst,
        );

        // Visibility for operators verifying primary↔replica parity
        // at a glance. SEC-03 cap and SEC-04 rate-limit knobs are
        // operator policy (not journaled), so logging the applied
        // values is the only way to confirm both processes started
        // with the same config.
        tracing::info!(
            max_orders_per_account = self.config.max_orders_per_account,
            max_orders_per_second = self.config.max_orders_per_second,
            max_orders_burst = self.config.max_orders_burst,
            "applied per-account order limits (SEC-03 cap, SEC-04 rate)"
        );
    }

    fn seed_events(&self) -> Vec<TradingEvent> {
        let mut events =
            Vec::with_capacity(self.config.instruments as usize + self.config.accounts as usize);
        for i in 0..self.config.instruments {
            events.push(TradingEvent::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(i),
                    base: CurrencyId(i * 2),
                    quote: CurrencyId(i * 2 + 1),
                },
            });
        }
        for acct in 1..=self.config.accounts {
            events.push(TradingEvent::ProvisionAccount {
                account: AccountId(acct),
                amount: u64::MAX / 4,
            });
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(accounts: u32, instruments: u32) -> FactoryConfig {
        FactoryConfig {
            accounts,
            instruments,
            max_orders_per_account: 100,
            max_orders_per_second: 1_000,
            max_orders_burst: 100,
        }
    }

    #[test]
    fn seed_events_count_matches_config() {
        let factory = Factory::new(cfg(5, 3));
        let events = factory.seed_events();
        // 3 instruments + 5 accounts.
        assert_eq!(events.len(), 8);
    }

    #[test]
    fn seed_events_order_is_instruments_then_accounts() {
        let factory = Factory::new(cfg(2, 2));
        let events = factory.seed_events();
        assert!(matches!(events[0], TradingEvent::AddInstrument { .. }));
        assert!(matches!(events[1], TradingEvent::AddInstrument { .. }));
        assert!(matches!(events[2], TradingEvent::ProvisionAccount { .. }));
        assert!(matches!(events[3], TradingEvent::ProvisionAccount { .. }));
    }

    #[test]
    fn seed_events_empty_when_no_accounts_or_instruments() {
        let factory = Factory::new(cfg(0, 0));
        assert!(factory.seed_events().is_empty());
    }

    #[test]
    fn empty_for_seed_applies_policy() {
        let factory = Factory::new(cfg(2, 2));
        let app = factory.empty_for_seed();
        // The configured cap (100) was applied, not the exchange
        // default (10_000).
        assert_eq!(app.max_open_orders_per_account(), 100);
    }

    #[test]
    fn empty_does_not_apply_policy() {
        let factory = Factory::new(cfg(2, 2));
        let app = factory.empty();
        // Fresh exchange — default cap, not the configured value.
        // Replication paths call `apply_operator_policy` explicitly
        // after `empty()` to converge primary/replica.
        assert_ne!(app.max_open_orders_per_account(), 100);
    }

    #[test]
    fn apply_operator_policy_overrides_default() {
        let factory = Factory::new(cfg(2, 2));
        let mut app = factory.empty();
        factory.apply_operator_policy(&mut app);
        assert_eq!(app.max_open_orders_per_account(), 100);
    }
}
