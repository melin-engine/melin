//! Trading-side [`AppFactory`] implementation.
//!
//! Owns the trading-domain construction recipe: empty / pre-sized
//! exchange, the SEC-03/SEC-04 operator-policy event, and the bulk-seed
//! `AddInstrument` / `ProvisionAccount` events. Moves all of it out
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

    fn prefault(&self, app: &mut ServerApp) {
        app.0.prefault_seed(
            self.config.accounts as usize,
            self.config.instruments as usize,
        );
    }

    fn operator_policy_event(&self) -> Option<TradingEvent> {
        // The SEC-03 cap and SEC-04 rate limit are journaled as a single
        // event so primary and replica converge by replay. The runtime
        // seeds this onto a fresh journal (via `seed_events`) and injects
        // it once when migrating a pre-feature lineage — see
        // `Application::operator_policy_present`. Always `Some`: the CLI
        // supplies concrete values (a `0` rate is a valid "disabled"
        // policy, still worth journaling so replicas match).
        //
        // Log the CLI-derived values here: this method is the CLI→journal
        // seam (called only at genesis seeding and pre-v19 migration, the
        // moments the CLI is authoritative), so it restores the
        // operator-facing visibility the removed `apply_operator_policy`
        // used to provide — the generic runtime can't log the values because
        // the event is opaque there.
        tracing::info!(
            max_open_orders_per_account = self.config.max_orders_per_account,
            max_orders_per_second = self.config.max_orders_per_second,
            max_orders_burst = self.config.max_orders_burst,
            "journaling operator policy from CLI (SEC-03 cap, SEC-04 rate)"
        );
        Some(TradingEvent::SetOperatorPolicy {
            max_open_orders_per_account: self.config.max_orders_per_account,
            max_orders_per_second: self.config.max_orders_per_second,
            max_orders_burst: self.config.max_orders_burst,
        })
    }

    fn seed_events(&self) -> Vec<TradingEvent> {
        let mut events = Vec::with_capacity(
            // +1 for the operator-policy event prepended below.
            1 + self.config.instruments as usize + self.config.accounts as usize,
        );
        // Operator policy first, so the rate-limit config is in force
        // before any seeded/inbound order is metered.
        if let Some(policy) = self.operator_policy_event() {
            events.push(policy);
        }
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
        // 1 operator policy + 3 instruments + 5 accounts.
        assert_eq!(events.len(), 9);
    }

    #[test]
    fn seed_events_order_is_policy_then_instruments_then_accounts() {
        let factory = Factory::new(cfg(2, 2));
        let events = factory.seed_events();
        assert!(matches!(events[0], TradingEvent::SetOperatorPolicy { .. }));
        assert!(matches!(events[1], TradingEvent::AddInstrument { .. }));
        assert!(matches!(events[2], TradingEvent::AddInstrument { .. }));
        assert!(matches!(events[3], TradingEvent::ProvisionAccount { .. }));
        assert!(matches!(events[4], TradingEvent::ProvisionAccount { .. }));
    }

    #[test]
    fn seed_events_carry_only_policy_when_no_accounts_or_instruments() {
        let factory = Factory::new(cfg(0, 0));
        let events = factory.seed_events();
        // Even an empty cluster journals the operator policy so a
        // later-joining replica converges on it by replay.
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], TradingEvent::SetOperatorPolicy { .. }));
    }

    #[test]
    fn empty_does_not_apply_policy() {
        let factory = Factory::new(cfg(2, 2));
        let app = factory.empty();
        // A blank exchange holds the engine default cap, not the CLI value —
        // the policy reaches the engine only through the journaled event.
        assert_ne!(app.max_open_orders_per_account(), 100);
        assert!(!app.operator_policy_set());
    }

    #[test]
    fn operator_policy_event_carries_config() {
        let factory = Factory::new(cfg(2, 2));
        match factory.operator_policy_event() {
            Some(TradingEvent::SetOperatorPolicy {
                max_open_orders_per_account,
                max_orders_per_second,
                max_orders_burst,
            }) => {
                assert_eq!(max_open_orders_per_account, 100);
                assert_eq!(max_orders_per_second, 1_000);
                assert_eq!(max_orders_burst, 100);
            }
            other => panic!("expected SetOperatorPolicy, got {other:?}"),
        }
    }
}
