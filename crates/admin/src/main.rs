//! Admin TUI — menu-driven command console for the trading engine.
//!
//! Connects to a running server with an Ed25519 key and provides a
//! wizard-style interface to send any request type. Responses are
//! displayed in a scrolling log behind the menu overlays.
//!
//! Usage:
//!     melin-admin <addr> <key-file>

use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::sync::mpsc;
use std::time::Instant;

use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph};

use melin_client::{Client, StatsSnapshot};
use melin_protocol::message::{Request, ResponseKind};
use melin_protocol::types::{
    AccountId, CircuitBreakerConfig, CurrencyId, ExecutionReport, FeeSchedule, InstrumentSpec,
    InstrumentStatus, Order, OrderId, OrderType, Price, Quantity, RejectReason, RiskLimits,
    SelfTradeProtection, Side, Symbol, TimeInForce,
};

// ── Menu definitions ────────────────────────────────────────────────

const ACTIONS: &[&str] = &[
    // Trading (0-7)
    "Limit Buy",
    "Limit Sell",
    "Market Buy",
    "Market Sell",
    "Stop Buy",
    "Stop Sell",
    "Stop-Limit Buy",
    "Stop-Limit Sell",
    // Cancel / Amend (8-10)
    "Cancel Order",
    "Cancel All",
    "Cancel-Replace",
    // Admin (11-15)
    "Add Instrument",
    "Deposit",
    "Set Risk Limits",
    "Set Circuit Breaker",
    "Set Fee Schedule",
    "End of Day",
    // Lifecycle (17-19)
    "Disable Instrument",
    "Enable Instrument",
    "Remove Instrument",
];

const TIF_OPTIONS: &[(&str, TimeInForce)] = &[
    ("GTC (Good-Til-Cancelled)", TimeInForce::GTC),
    ("IOC (Immediate-Or-Cancel)", TimeInForce::IOC),
    ("FOK (Fill-Or-Kill)", TimeInForce::FOK),
    ("Day (cancel at end-of-day)", TimeInForce::Day),
    ("GTD (Good-Til-Date)", TimeInForce::GTD),
];

const STP_OPTIONS: &[(&str, SelfTradeProtection)] = &[
    ("Cancel Newest (default)", SelfTradeProtection::CancelNewest),
    ("Cancel Oldest", SelfTradeProtection::CancelOldest),
    ("Cancel Both", SelfTradeProtection::CancelBoth),
    ("Allow", SelfTradeProtection::Allow),
];

// ── App state ───────────────────────────────────────────────────────

/// Which screen/step the user is on.
enum Screen {
    /// Top-level action menu.
    ActionMenu,
    /// Entering a number. `label` describes the field, `next` says
    /// what to do when Enter is pressed with a valid value.
    NumberInput {
        label: &'static str,
        buf: String,
        next: NextStep,
    },
    /// Picking time-in-force.
    TifMenu { collected: OrderFields },
    /// Picking self-trade prevention mode.
    StpMenu { collected: OrderFields },
    /// Entering the text command directly (toggle with Tab).
    CommandInput { buf: String },
}

/// Accumulated order fields as we progress through the wizard.
#[derive(Clone)]
struct OrderFields {
    action: usize,
    symbol: u32,
    account: u32,
    tif: TimeInForce,
    stp: SelfTradeProtection,
    /// Limit price (for limit and stop-limit orders).
    price: Option<u64>,
    /// Trigger price (for stop and stop-limit orders).
    trigger_price: Option<u64>,
    /// Quantity in lots.
    quantity: Option<u64>,
    /// Expiry timestamp in nanoseconds since Unix epoch (GTD only).
    expiry_ns: u64,
}

impl OrderFields {
    fn new(action: usize) -> Self {
        Self {
            action,
            symbol: 0,
            account: 0,
            tif: TimeInForce::GTC,
            stp: SelfTradeProtection::default(),
            price: None,
            trigger_price: None,
            quantity: None,
            expiry_ns: 0,
        }
    }

    fn is_limit(&self) -> bool {
        self.action <= 1
    }

    fn is_market(&self) -> bool {
        self.action == 2 || self.action == 3
    }

    fn is_stop(&self) -> bool {
        self.action == 4 || self.action == 5
    }

    fn is_stop_limit(&self) -> bool {
        self.action == 6 || self.action == 7
    }

    fn side(&self) -> Side {
        if self.action.is_multiple_of(2) {
            Side::Buy
        } else {
            Side::Sell
        }
    }
}

/// What to do after a number input is accepted.
#[derive(Clone)]
enum NextStep {
    /// We just entered the symbol — next ask for account.
    Account { collected: OrderFields },
    /// We just entered the account — next depends on order type.
    AfterAccount { collected: OrderFields },
    /// We just entered the trigger price — ask for limit price (stop-limit)
    /// or quantity (stop).
    AfterTrigger { collected: OrderFields },
    /// We just entered the limit price — ask for quantity.
    AfterPrice { collected: OrderFields },
    /// We just entered the quantity — go to TIF (limit) or submit (market/stop).
    AfterQuantity { collected: OrderFields },
    /// Cancel order: entered symbol, now enter account ID.
    CancelOrderAccount { symbol: u32 },
    /// Cancel order: entered account, now enter order ID.
    CancelOrderId { symbol: u32, account: u32 },
    /// Cancel all: enter account ID.
    CancelAllAccount,
    // --- Admin flows ---
    /// Add instrument: entered symbol, now enter base currency.
    AddInstrumentBase { symbol: u32 },
    /// Add instrument: entered base, now enter quote currency.
    AddInstrumentQuote { symbol: u32, base: u32 },
    /// Deposit: enter account ID.
    DepositAccount,
    /// Deposit: entered account, now enter currency ID.
    DepositCurrency { account: u32 },
    /// Deposit: entered currency, now enter amount.
    DepositAmount { account: u32, currency: u32 },
    /// Set risk limits: enter symbol.
    RiskLimitsSymbol,
    /// Set risk limits: entered symbol, now enter max order qty (0 to skip).
    RiskLimitsMaxQty { symbol: u32 },
    /// Set risk limits: entered max qty, now enter max notional (0 to skip).
    RiskLimitsMaxNotional {
        symbol: u32,
        max_order_qty: Option<u64>,
    },
    /// Cancel-replace: enter symbol.
    CancelReplaceSymbol,
    /// Cancel-replace: entered symbol, enter account ID.
    CancelReplaceAccount { symbol: u32 },
    /// Cancel-replace: entered account, enter order ID.
    CancelReplaceOrderId { symbol: u32, account: u32 },
    /// Cancel-replace: entered order ID, enter new price.
    CancelReplacePrice {
        symbol: u32,
        account: u32,
        order_id: u64,
    },
    /// Cancel-replace: entered price, enter new quantity.
    CancelReplaceQty {
        symbol: u32,
        account: u32,
        order_id: u64,
        new_price: u64,
    },
    /// Circuit breaker: enter symbol.
    CircuitBreakerSymbol,
    /// Circuit breaker: entered symbol, enter lower price band (0 = none).
    CircuitBreakerLower { symbol: u32 },
    /// Circuit breaker: entered lower, enter upper price band (0 = none).
    CircuitBreakerUpper { symbol: u32, lower: Option<u64> },
    /// Circuit breaker: entered upper, enter halted (0 = no, 1 = yes).
    CircuitBreakerHalted {
        symbol: u32,
        lower: Option<u64>,
        upper: Option<u64>,
    },
    /// Fee schedule: enter symbol.
    FeeScheduleSymbol,
    /// Fee schedule: entered symbol, enter maker fee bps.
    FeeScheduleMakerBps { symbol: u32 },
    /// Fee schedule: entered maker bps, enter taker fee bps.
    /// `i16` to support negative values (rebates).
    FeeScheduleTakerBps { symbol: u32, maker_bps: i16 },
    /// Disable instrument: enter symbol ID.
    DisableInstrumentSymbol,
    /// Enable instrument: enter symbol ID.
    EnableInstrumentSymbol,
    /// Remove instrument: enter symbol ID.
    RemoveInstrumentSymbol,
    /// GTD expiry: after TIF selection, enter expiry timestamp_ns.
    AfterTifExpiry { collected: OrderFields },
}

struct App {
    screen: Screen,
    cursor: usize,
    log: Vec<String>,
    request_tx: mpsc::Sender<Request>,
    response_rx: mpsc::Receiver<String>,
    quit: bool,
    next_order_id: u64,
    /// Last stats snapshot received from the server.
    stats: Option<DashboardStats>,
    /// When the last stats poll was sent. Used to send QueryStats every second.
    last_stats_poll: Instant,
}

/// Dashboard stats computed from `StatsHeader` responses.
struct DashboardStats {
    active_connections: u64,
    events_processed: u64,
    journal_sequence: u64,
    /// Computed throughput: events/sec since last snapshot.
    throughput: u64,
}

impl App {
    fn new(request_tx: mpsc::Sender<Request>, response_rx: mpsc::Receiver<String>) -> Self {
        Self {
            screen: Screen::ActionMenu,
            cursor: 0,
            log: vec!["Select an action with ↑/↓ and Enter. Tab for text command mode.".into()],
            request_tx,
            response_rx,
            quit: false,
            next_order_id: 1,
            stats: None,
            last_stats_poll: Instant::now(),
        }
    }

    fn poll_responses(&mut self) {
        while let Ok(msg) = self.response_rx.try_recv() {
            self.log.push(msg);
            if self.log.len() > 10_000 {
                self.log.drain(..5_000);
            }
        }
    }

    fn menu_len(&self) -> usize {
        match &self.screen {
            Screen::ActionMenu => ACTIONS.len(),
            Screen::TifMenu { .. } => TIF_OPTIONS.len(),
            Screen::StpMenu { .. } => STP_OPTIONS.len(),
            _ => 0,
        }
    }

    fn move_up(&mut self) {
        let len = self.menu_len();
        if len > 0 {
            self.cursor = (self.cursor + len - 1) % len;
        }
    }

    fn move_down(&mut self) {
        let len = self.menu_len();
        if len > 0 {
            self.cursor = (self.cursor + 1) % len;
        }
    }

    fn go_back(&mut self) {
        self.screen = match &self.screen {
            Screen::ActionMenu => {
                self.quit = true;
                return;
            }
            Screen::CommandInput { .. } => Screen::ActionMenu,
            Screen::TifMenu { collected } => {
                // Back from TIF → re-enter quantity.
                Screen::NumberInput {
                    label: "Quantity",
                    buf: String::new(),
                    next: NextStep::AfterQuantity {
                        collected: collected.clone(),
                    },
                }
            }
            Screen::StpMenu { collected } => {
                // Back from STP → re-pick TIF.
                Screen::TifMenu {
                    collected: collected.clone(),
                }
            }
            Screen::NumberInput { next, .. } => {
                // Back from number input → depends on which step we're on.
                match next {
                    // First step of any flow → back to action menu.
                    NextStep::Account { .. }
                    | NextStep::CancelOrderAccount { symbol: 0 }
                    | NextStep::CancelAllAccount
                    | NextStep::AddInstrumentBase { symbol: 0 }
                    | NextStep::DepositAccount
                    | NextStep::RiskLimitsSymbol
                    | NextStep::CancelReplaceSymbol
                    | NextStep::CircuitBreakerSymbol
                    | NextStep::FeeScheduleSymbol => Screen::ActionMenu,
                    // Deeper steps → back to previous input.
                    NextStep::AfterAccount { collected } => Screen::NumberInput {
                        label: "Symbol ID",
                        buf: String::new(),
                        next: NextStep::Account {
                            collected: collected.clone(),
                        },
                    },
                    NextStep::AfterTrigger { collected } => Screen::NumberInput {
                        label: "Account ID",
                        buf: String::new(),
                        next: NextStep::AfterAccount {
                            collected: collected.clone(),
                        },
                    },
                    NextStep::AfterPrice { collected } => {
                        if collected.is_stop_limit() {
                            Screen::NumberInput {
                                label: "Trigger Price",
                                buf: String::new(),
                                next: NextStep::AfterTrigger {
                                    collected: collected.clone(),
                                },
                            }
                        } else {
                            Screen::NumberInput {
                                label: "Account ID",
                                buf: String::new(),
                                next: NextStep::AfterAccount {
                                    collected: collected.clone(),
                                },
                            }
                        }
                    }
                    NextStep::AfterQuantity { collected } => {
                        if collected.price.is_some() {
                            Screen::NumberInput {
                                label: "Limit Price",
                                buf: String::new(),
                                next: NextStep::AfterPrice {
                                    collected: collected.clone(),
                                },
                            }
                        } else if collected.trigger_price.is_some() {
                            Screen::NumberInput {
                                label: "Trigger Price",
                                buf: String::new(),
                                next: NextStep::AfterTrigger {
                                    collected: collected.clone(),
                                },
                            }
                        } else {
                            Screen::NumberInput {
                                label: "Account ID",
                                buf: String::new(),
                                next: NextStep::AfterAccount {
                                    collected: collected.clone(),
                                },
                            }
                        }
                    }
                    NextStep::CancelOrderAccount { .. } => Screen::NumberInput {
                        label: "Symbol ID",
                        buf: String::new(),
                        next: NextStep::CancelOrderAccount { symbol: 0 },
                    },
                    NextStep::CancelOrderId { symbol, .. } => Screen::NumberInput {
                        label: "Account ID",
                        buf: String::new(),
                        next: NextStep::CancelOrderAccount { symbol: *symbol },
                    },
                    NextStep::AddInstrumentBase { .. } => Screen::NumberInput {
                        label: "Symbol ID",
                        buf: String::new(),
                        next: NextStep::AddInstrumentBase { symbol: 0 },
                    },
                    NextStep::AddInstrumentQuote { symbol, .. } => Screen::NumberInput {
                        label: "Base Currency ID",
                        buf: String::new(),
                        next: NextStep::AddInstrumentBase { symbol: *symbol },
                    },
                    NextStep::DepositCurrency { .. } => Screen::NumberInput {
                        label: "Account ID",
                        buf: String::new(),
                        next: NextStep::DepositAccount,
                    },
                    NextStep::DepositAmount { account, .. } => Screen::NumberInput {
                        label: "Currency ID",
                        buf: String::new(),
                        next: NextStep::DepositCurrency { account: *account },
                    },
                    NextStep::RiskLimitsMaxQty { .. } => Screen::NumberInput {
                        label: "Symbol ID",
                        buf: String::new(),
                        next: NextStep::RiskLimitsSymbol,
                    },
                    NextStep::RiskLimitsMaxNotional { symbol, .. } => Screen::NumberInput {
                        label: "Max Order Qty (0 = no limit)",
                        buf: String::new(),
                        next: NextStep::RiskLimitsMaxQty { symbol: *symbol },
                    },
                    NextStep::CancelReplaceAccount { .. } => Screen::NumberInput {
                        label: "Symbol ID",
                        buf: String::new(),
                        next: NextStep::CancelReplaceSymbol,
                    },
                    NextStep::CancelReplaceOrderId { symbol, .. } => Screen::NumberInput {
                        label: "Account ID",
                        buf: String::new(),
                        next: NextStep::CancelReplaceAccount { symbol: *symbol },
                    },
                    NextStep::CancelReplacePrice {
                        symbol, account, ..
                    } => Screen::NumberInput {
                        label: "Order ID",
                        buf: String::new(),
                        next: NextStep::CancelReplaceOrderId {
                            symbol: *symbol,
                            account: *account,
                        },
                    },
                    NextStep::CancelReplaceQty {
                        symbol,
                        account,
                        order_id,
                        ..
                    } => Screen::NumberInput {
                        label: "New Price",
                        buf: String::new(),
                        next: NextStep::CancelReplacePrice {
                            symbol: *symbol,
                            account: *account,
                            order_id: *order_id,
                        },
                    },
                    NextStep::CircuitBreakerLower { .. } => Screen::NumberInput {
                        label: "Symbol ID",
                        buf: String::new(),
                        next: NextStep::CircuitBreakerSymbol,
                    },
                    NextStep::CircuitBreakerUpper { symbol, .. } => Screen::NumberInput {
                        label: "Lower Price Band (0 = none)",
                        buf: String::new(),
                        next: NextStep::CircuitBreakerLower { symbol: *symbol },
                    },
                    NextStep::CircuitBreakerHalted { symbol, lower, .. } => Screen::NumberInput {
                        label: "Upper Price Band (0 = none)",
                        buf: String::new(),
                        next: NextStep::CircuitBreakerUpper {
                            symbol: *symbol,
                            lower: *lower,
                        },
                    },
                    NextStep::FeeScheduleMakerBps { .. } => Screen::NumberInput {
                        label: "Symbol ID",
                        buf: String::new(),
                        next: NextStep::FeeScheduleSymbol,
                    },
                    NextStep::FeeScheduleTakerBps { symbol, .. } => Screen::NumberInput {
                        label: "Maker Fee (bps, -10000..10000, negative=rebate)",
                        buf: String::new(),
                        next: NextStep::FeeScheduleMakerBps { symbol: *symbol },
                    },
                    NextStep::DisableInstrumentSymbol => Screen::ActionMenu,
                    NextStep::EnableInstrumentSymbol => Screen::ActionMenu,
                    NextStep::RemoveInstrumentSymbol => Screen::ActionMenu,
                    NextStep::AfterTifExpiry { collected } => Screen::TifMenu {
                        collected: collected.clone(),
                    },
                }
            }
        };
        self.cursor = 0;
    }

    fn select(&mut self) {
        match &self.screen {
            Screen::ActionMenu => {
                let action = self.cursor;
                match action {
                    0..=7 => {
                        // Order type — start with symbol input.
                        let collected = OrderFields::new(action);
                        self.screen = Screen::NumberInput {
                            label: "Symbol ID",
                            buf: String::new(),
                            next: NextStep::Account { collected },
                        };
                    }
                    8 => {
                        // Cancel Order — ask for symbol.
                        self.screen = Screen::NumberInput {
                            label: "Symbol ID",
                            buf: String::new(),
                            next: NextStep::CancelOrderAccount { symbol: 0 },
                        };
                    }
                    9 => {
                        // Cancel All — ask for account.
                        self.screen = Screen::NumberInput {
                            label: "Account ID",
                            buf: String::new(),
                            next: NextStep::CancelAllAccount,
                        };
                    }
                    10 => {
                        // Cancel-Replace — ask for symbol.
                        self.screen = Screen::NumberInput {
                            label: "Symbol ID",
                            buf: String::new(),
                            next: NextStep::CancelReplaceSymbol,
                        };
                    }
                    11 => {
                        // Add Instrument — ask for symbol ID.
                        self.screen = Screen::NumberInput {
                            label: "Symbol ID",
                            buf: String::new(),
                            next: NextStep::AddInstrumentBase { symbol: 0 },
                        };
                    }
                    12 => {
                        // Deposit — ask for account ID.
                        self.screen = Screen::NumberInput {
                            label: "Account ID",
                            buf: String::new(),
                            next: NextStep::DepositAccount,
                        };
                    }
                    13 => {
                        // Set Risk Limits — ask for symbol.
                        self.screen = Screen::NumberInput {
                            label: "Symbol ID",
                            buf: String::new(),
                            next: NextStep::RiskLimitsSymbol,
                        };
                    }
                    14 => {
                        // Set Circuit Breaker — ask for symbol.
                        self.screen = Screen::NumberInput {
                            label: "Symbol ID",
                            buf: String::new(),
                            next: NextStep::CircuitBreakerSymbol,
                        };
                    }
                    15 => {
                        // Set Fee Schedule — ask for symbol.
                        self.screen = Screen::NumberInput {
                            label: "Symbol ID",
                            buf: String::new(),
                            next: NextStep::FeeScheduleSymbol,
                        };
                    }
                    16 => {
                        // End of Day — send immediately, no parameters.
                        let _ = self.request_tx.send(Request::EndOfDay);
                        self.log.push("Sent EndOfDay".into());
                    }
                    17 => {
                        // Disable Instrument — ask for symbol.
                        self.screen = Screen::NumberInput {
                            label: "Symbol ID",
                            buf: String::new(),
                            next: NextStep::DisableInstrumentSymbol,
                        };
                    }
                    18 => {
                        // Enable Instrument — ask for symbol.
                        self.screen = Screen::NumberInput {
                            label: "Symbol ID",
                            buf: String::new(),
                            next: NextStep::EnableInstrumentSymbol,
                        };
                    }
                    19 => {
                        // Remove Instrument — ask for symbol.
                        self.screen = Screen::NumberInput {
                            label: "Symbol ID",
                            buf: String::new(),
                            next: NextStep::RemoveInstrumentSymbol,
                        };
                    }
                    _ => {}
                }
            }

            Screen::NumberInput { buf, next, .. } => {
                // Fee schedule fields allow negative values (rebates), so
                // parse as i64 first and then convert. All other fields
                // require non-negative values parsed as u64.
                let is_fee_field = matches!(
                    next,
                    NextStep::FeeScheduleMakerBps { .. } | NextStep::FeeScheduleTakerBps { .. }
                );
                let val: u64 = if is_fee_field {
                    // Parse as i64 to allow negatives, then store as u64
                    // (the fee-specific match arms will re-interpret via `as i16`).
                    match buf.parse::<i64>() {
                        Ok(v) if (-10_000..=10_000).contains(&v) => v as u64,
                        _ => {
                            self.log
                                .push("Invalid fee (expected -10000..10000).".into());
                            return;
                        }
                    }
                } else {
                    match buf.parse() {
                        Ok(v) => v,
                        _ => {
                            self.log.push("Invalid input (expected a number).".into());
                            return;
                        }
                    }
                };
                // Most fields require > 0 (symbol, account, price, qty).
                // Risk limit fields allow 0 (meaning "no limit").
                // Fee fields are handled above with signed parsing.
                let allows_zero = matches!(
                    next,
                    NextStep::RiskLimitsMaxQty { .. }
                        | NextStep::RiskLimitsMaxNotional { .. }
                        | NextStep::CircuitBreakerLower { .. }
                        | NextStep::CircuitBreakerUpper { .. }
                        | NextStep::CircuitBreakerHalted { .. }
                        | NextStep::FeeScheduleMakerBps { .. }
                        | NextStep::FeeScheduleTakerBps { .. }
                );
                if val == 0 && !allows_zero {
                    self.log.push("Value must be > 0.".into());
                    return;
                }

                match next.clone() {
                    NextStep::Account { mut collected } => {
                        collected.symbol = val as u32;
                        self.screen = Screen::NumberInput {
                            label: "Account ID",
                            buf: String::new(),
                            next: NextStep::AfterAccount { collected },
                        };
                    }
                    NextStep::AfterAccount { mut collected } => {
                        collected.account = val as u32;
                        if collected.is_stop() || collected.is_stop_limit() {
                            self.screen = Screen::NumberInput {
                                label: "Trigger Price",
                                buf: String::new(),
                                next: NextStep::AfterTrigger { collected },
                            };
                        } else if collected.is_limit() {
                            self.screen = Screen::NumberInput {
                                label: "Limit Price",
                                buf: String::new(),
                                next: NextStep::AfterPrice { collected },
                            };
                        } else {
                            // Market — skip price, go to quantity.
                            self.screen = Screen::NumberInput {
                                label: "Quantity",
                                buf: String::new(),
                                next: NextStep::AfterQuantity { collected },
                            };
                        }
                    }
                    NextStep::AfterTrigger { mut collected } => {
                        collected.trigger_price = Some(val);
                        if collected.is_stop_limit() {
                            self.screen = Screen::NumberInput {
                                label: "Limit Price",
                                buf: String::new(),
                                next: NextStep::AfterPrice { collected },
                            };
                        } else {
                            // Plain stop — go to quantity.
                            self.screen = Screen::NumberInput {
                                label: "Quantity",
                                buf: String::new(),
                                next: NextStep::AfterQuantity { collected },
                            };
                        }
                    }
                    NextStep::AfterPrice { mut collected } => {
                        collected.price = Some(val);
                        self.screen = Screen::NumberInput {
                            label: "Quantity",
                            buf: String::new(),
                            next: NextStep::AfterQuantity { collected },
                        };
                    }
                    NextStep::AfterQuantity { mut collected } => {
                        collected.quantity = Some(val);
                        if collected.is_market() || collected.is_stop() {
                            // Market/stop orders: skip TIF/STP, submit directly.
                            self.submit_order(&collected);
                            self.screen = Screen::ActionMenu;
                            self.cursor = 0;
                        } else {
                            // Limit/stop-limit: pick TIF.
                            self.screen = Screen::TifMenu { collected };
                            self.cursor = 0;
                        }
                    }
                    NextStep::CancelOrderAccount { symbol: 0 } => {
                        // First pass: `val` is the symbol.
                        self.screen = Screen::NumberInput {
                            label: "Account ID",
                            buf: String::new(),
                            next: NextStep::CancelOrderAccount { symbol: val as u32 },
                        };
                    }
                    NextStep::CancelOrderAccount { symbol } => {
                        // Second pass: `val` is the account.
                        self.screen = Screen::NumberInput {
                            label: "Order ID",
                            buf: String::new(),
                            next: NextStep::CancelOrderId {
                                symbol,
                                account: val as u32,
                            },
                        };
                    }
                    NextStep::CancelOrderId { symbol, account } => {
                        let request = Request::CancelOrder {
                            symbol: Symbol(symbol),
                            account: AccountId(account),
                            order_id: OrderId(val),
                        };
                        self.log.push(format!(
                            "→ CANCEL #{val} on symbol {} account {}",
                            symbol, account
                        ));
                        let _ = self.request_tx.send(request);
                        self.screen = Screen::ActionMenu;
                        self.cursor = 0;
                    }
                    NextStep::CancelAllAccount => {
                        let request = Request::CancelAll {
                            account: AccountId(val as u32),
                        };
                        self.log.push(format!("→ CANCEL ALL for account {val}"));
                        let _ = self.request_tx.send(request);
                        self.screen = Screen::ActionMenu;
                        self.cursor = 0;
                    }

                    // --- Admin flows ---
                    NextStep::AddInstrumentBase { symbol: 0 } => {
                        // First pass: `val` is the symbol ID.
                        self.screen = Screen::NumberInput {
                            label: "Base Currency ID",
                            buf: String::new(),
                            next: NextStep::AddInstrumentBase { symbol: val as u32 },
                        };
                    }
                    NextStep::AddInstrumentBase { symbol } => {
                        self.screen = Screen::NumberInput {
                            label: "Quote Currency ID",
                            buf: String::new(),
                            next: NextStep::AddInstrumentQuote {
                                symbol,
                                base: val as u32,
                            },
                        };
                    }
                    NextStep::AddInstrumentQuote { symbol, base } => {
                        let request = Request::AddInstrument {
                            spec: InstrumentSpec {
                                symbol: Symbol(symbol),
                                base: CurrencyId(base),
                                quote: CurrencyId(val as u32),
                            },
                        };
                        self.log.push(format!(
                            "→ ADD INSTRUMENT sym:{} base:{} quote:{}",
                            symbol, base, val
                        ));
                        let _ = self.request_tx.send(request);
                        self.screen = Screen::ActionMenu;
                        self.cursor = 0;
                    }
                    NextStep::DepositAccount => {
                        self.screen = Screen::NumberInput {
                            label: "Currency ID",
                            buf: String::new(),
                            next: NextStep::DepositCurrency {
                                account: val as u32,
                            },
                        };
                    }
                    NextStep::DepositCurrency { account } => {
                        self.screen = Screen::NumberInput {
                            label: "Amount",
                            buf: String::new(),
                            next: NextStep::DepositAmount {
                                account,
                                currency: val as u32,
                            },
                        };
                    }
                    NextStep::DepositAmount { account, currency } => {
                        let request = Request::Deposit {
                            account: AccountId(account),
                            currency: CurrencyId(currency),
                            amount: val,
                        };
                        self.log.push(format!(
                            "→ DEPOSIT acct:{} currency:{} amount:{}",
                            account, currency, val
                        ));
                        let _ = self.request_tx.send(request);
                        self.screen = Screen::ActionMenu;
                        self.cursor = 0;
                    }
                    NextStep::RiskLimitsSymbol => {
                        self.screen = Screen::NumberInput {
                            label: "Max Order Qty (0 = no limit)",
                            buf: String::new(),
                            next: NextStep::RiskLimitsMaxQty { symbol: val as u32 },
                        };
                    }
                    NextStep::RiskLimitsMaxQty { symbol } => {
                        let max_qty = if val == 0 { None } else { Some(val) };
                        self.screen = Screen::NumberInput {
                            label: "Max Notional (0 = no limit)",
                            buf: String::new(),
                            next: NextStep::RiskLimitsMaxNotional {
                                symbol,
                                max_order_qty: max_qty,
                            },
                        };
                    }
                    NextStep::RiskLimitsMaxNotional {
                        symbol,
                        max_order_qty,
                    } => {
                        let max_notional = if val == 0 { None } else { Some(val) };
                        let limits = RiskLimits {
                            max_order_qty: max_order_qty
                                .map(|v| Quantity(NonZeroU64::new(v).expect("validated > 0"))),
                            max_order_notional: max_notional,
                        };
                        let request = Request::SetRiskLimits {
                            symbol: Symbol(symbol),
                            limits,
                        };
                        self.log.push(format!(
                            "→ SET RISK LIMITS sym:{} max_qty:{} max_notional:{}",
                            symbol,
                            max_order_qty
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "none".into()),
                            max_notional
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "none".into()),
                        ));
                        let _ = self.request_tx.send(request);
                        self.screen = Screen::ActionMenu;
                        self.cursor = 0;
                    }

                    // --- Cancel-replace flow ---
                    NextStep::CancelReplaceSymbol => {
                        self.screen = Screen::NumberInput {
                            label: "Account ID",
                            buf: String::new(),
                            next: NextStep::CancelReplaceAccount { symbol: val as u32 },
                        };
                    }
                    NextStep::CancelReplaceAccount { symbol } => {
                        self.screen = Screen::NumberInput {
                            label: "Order ID",
                            buf: String::new(),
                            next: NextStep::CancelReplaceOrderId {
                                symbol,
                                account: val as u32,
                            },
                        };
                    }
                    NextStep::CancelReplaceOrderId { symbol, account } => {
                        self.screen = Screen::NumberInput {
                            label: "New Price",
                            buf: String::new(),
                            next: NextStep::CancelReplacePrice {
                                symbol,
                                account,
                                order_id: val,
                            },
                        };
                    }
                    NextStep::CancelReplacePrice {
                        symbol,
                        account,
                        order_id,
                    } => {
                        self.screen = Screen::NumberInput {
                            label: "New Quantity (remaining)",
                            buf: String::new(),
                            next: NextStep::CancelReplaceQty {
                                symbol,
                                account,
                                order_id,
                                new_price: val,
                            },
                        };
                    }
                    NextStep::CancelReplaceQty {
                        symbol,
                        account,
                        order_id,
                        new_price,
                    } => {
                        let request = Request::CancelReplace {
                            symbol: Symbol(symbol),
                            account: AccountId(account),
                            order_id: OrderId(order_id),
                            new_price: Price(NonZeroU64::new(new_price).expect("validated > 0")),
                            new_quantity: Quantity(NonZeroU64::new(val).expect("validated > 0")),
                        };
                        self.log.push(format!(
                            "→ CANCEL-REPLACE #{} sym:{} acct:{} price:{} qty:{}",
                            order_id, symbol, account, new_price, val
                        ));
                        let _ = self.request_tx.send(request);
                        self.screen = Screen::ActionMenu;
                        self.cursor = 0;
                    }

                    // --- Circuit breaker flow ---
                    NextStep::CircuitBreakerSymbol => {
                        self.screen = Screen::NumberInput {
                            label: "Lower Price Band (0 = none)",
                            buf: String::new(),
                            next: NextStep::CircuitBreakerLower { symbol: val as u32 },
                        };
                    }
                    NextStep::CircuitBreakerLower { symbol } => {
                        let lower = if val == 0 { None } else { Some(val) };
                        self.screen = Screen::NumberInput {
                            label: "Upper Price Band (0 = none)",
                            buf: String::new(),
                            next: NextStep::CircuitBreakerUpper { symbol, lower },
                        };
                    }
                    NextStep::CircuitBreakerUpper { symbol, lower } => {
                        let upper = if val == 0 { None } else { Some(val) };
                        self.screen = Screen::NumberInput {
                            label: "Halted? (0 = no, 1 = yes)",
                            buf: String::new(),
                            next: NextStep::CircuitBreakerHalted {
                                symbol,
                                lower,
                                upper,
                            },
                        };
                    }
                    NextStep::CircuitBreakerHalted {
                        symbol,
                        lower,
                        upper,
                    } => {
                        let config = CircuitBreakerConfig {
                            price_band_lower: lower
                                .map(|v| Price(NonZeroU64::new(v).expect("validated > 0"))),
                            price_band_upper: upper
                                .map(|v| Price(NonZeroU64::new(v).expect("validated > 0"))),
                            halted: val != 0,
                        };
                        let request = Request::SetCircuitBreaker {
                            symbol: Symbol(symbol),
                            config,
                        };
                        self.log.push(format!(
                            "→ CIRCUIT BREAKER sym:{} lower:{} upper:{} halted:{}",
                            symbol,
                            lower
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "none".into()),
                            upper
                                .map(|v| v.to_string())
                                .unwrap_or_else(|| "none".into()),
                            config.halted,
                        ));
                        let _ = self.request_tx.send(request);
                        self.screen = Screen::ActionMenu;
                        self.cursor = 0;
                    }

                    // --- Fee schedule flow ---
                    NextStep::FeeScheduleSymbol => {
                        self.screen = Screen::NumberInput {
                            label: "Maker Fee (bps, -10000..10000, negative=rebate)",
                            buf: String::new(),
                            next: NextStep::FeeScheduleMakerBps { symbol: val as u32 },
                        };
                    }
                    NextStep::FeeScheduleMakerBps { symbol } => {
                        self.screen = Screen::NumberInput {
                            label: "Taker Fee (bps, -10000..10000, negative=rebate)",
                            buf: String::new(),
                            next: NextStep::FeeScheduleTakerBps {
                                symbol,
                                maker_bps: val as i16,
                            },
                        };
                    }
                    NextStep::FeeScheduleTakerBps { symbol, maker_bps } => {
                        let taker_bps = val as i16;
                        let request = Request::SetFeeSchedule {
                            symbol: Symbol(symbol),
                            schedule: FeeSchedule {
                                maker_fee_bps: maker_bps,
                                taker_fee_bps: taker_bps,
                            },
                        };
                        self.log.push(format!(
                            "→ FEE SCHEDULE sym:{} maker:{}bps taker:{}bps",
                            symbol, maker_bps, taker_bps
                        ));
                        let _ = self.request_tx.send(request);
                        self.screen = Screen::ActionMenu;
                        self.cursor = 0;
                    }

                    // --- Instrument lifecycle flows ---
                    NextStep::DisableInstrumentSymbol => {
                        let request = Request::DisableInstrument {
                            symbol: Symbol(val as u32),
                        };
                        self.log.push(format!("→ DISABLE INSTRUMENT symbol={val}"));
                        let _ = self.request_tx.send(request);
                        self.screen = Screen::ActionMenu;
                        self.cursor = 0;
                    }
                    NextStep::EnableInstrumentSymbol => {
                        let request = Request::EnableInstrument {
                            symbol: Symbol(val as u32),
                        };
                        self.log.push(format!("→ ENABLE INSTRUMENT symbol={val}"));
                        let _ = self.request_tx.send(request);
                        self.screen = Screen::ActionMenu;
                        self.cursor = 0;
                    }
                    NextStep::RemoveInstrumentSymbol => {
                        let request = Request::RemoveInstrument {
                            symbol: Symbol(val as u32),
                        };
                        self.log.push(format!("→ REMOVE INSTRUMENT symbol={val}"));
                        let _ = self.request_tx.send(request);
                        self.screen = Screen::ActionMenu;
                        self.cursor = 0;
                    }

                    // --- GTD expiry input after TIF selection ---
                    NextStep::AfterTifExpiry { mut collected } => {
                        collected.expiry_ns = val;
                        self.screen = Screen::StpMenu { collected };
                        self.cursor = 0;
                    }
                }
            }

            Screen::TifMenu { collected } => {
                let mut collected = collected.clone();
                collected.tif = TIF_OPTIONS[self.cursor].1;
                if collected.tif == TimeInForce::GTD {
                    // GTD requires an expiry timestamp — prompt for it.
                    self.screen = Screen::NumberInput {
                        label: "Expiry (ns since Unix epoch)",
                        buf: String::new(),
                        next: NextStep::AfterTifExpiry { collected },
                    };
                } else {
                    self.screen = Screen::StpMenu { collected };
                }
                self.cursor = 0;
            }

            Screen::StpMenu { collected } => {
                let mut collected = collected.clone();
                collected.stp = STP_OPTIONS[self.cursor].1;
                self.submit_order(&collected);
                self.screen = Screen::ActionMenu;
                self.cursor = 0;
            }

            Screen::CommandInput { buf } => {
                let cmd = buf.trim().to_string();
                if cmd.is_empty() {
                    return;
                }
                self.log.push(format!("> {cmd}"));
                match parse_text_command(&cmd) {
                    Ok(request) => {
                        let _ = self.request_tx.send(request);
                    }
                    Err(e) => {
                        self.log.push(format!("ERROR: {e}"));
                    }
                }
                if let Screen::CommandInput { buf } = &mut self.screen {
                    buf.clear();
                }
            }
        }
    }

    fn submit_order(&mut self, f: &OrderFields) {
        let order_id = OrderId(self.next_order_id);
        self.next_order_id += 1;

        let order_type = if let Some(trigger) = f.trigger_price {
            if let Some(limit) = f.price {
                OrderType::StopLimit {
                    trigger_price: Price(NonZeroU64::new(trigger).expect("validated > 0")),
                    limit_price: Price(NonZeroU64::new(limit).expect("validated > 0")),
                }
            } else {
                OrderType::Stop {
                    trigger_price: Price(NonZeroU64::new(trigger).expect("validated > 0")),
                }
            }
        } else if let Some(p) = f.price {
            OrderType::Limit {
                price: Price(NonZeroU64::new(p).expect("validated > 0")),
                post_only: false,
            }
        } else {
            OrderType::Market
        };

        let qty = Quantity(NonZeroU64::new(f.quantity.expect("validated")).expect("validated > 0"));

        let order = Order {
            id: order_id,
            account: AccountId(f.account),
            side: f.side(),
            order_type,
            time_in_force: f.tif,
            quantity: qty,
            stp: f.stp,
            expiry_ns: f.expiry_ns,
        };

        let side_str = if f.side() == Side::Buy { "BUY" } else { "SELL" };
        let type_str = match &order_type {
            OrderType::Market => "MARKET".into(),
            OrderType::Limit { price, .. } => format!("LIMIT @{}", price.0),
            OrderType::Stop { trigger_price } => format!("STOP trigger @{}", trigger_price.0),
            OrderType::StopLimit {
                trigger_price,
                limit_price,
            } => {
                format!(
                    "STOP-LIMIT trigger @{} limit @{}",
                    trigger_price.0, limit_price.0
                )
            }
        };
        self.log.push(format!(
            "→ {side_str} {type_str} sym:{} acct:{} x{} (order #{})",
            f.symbol, f.account, qty.0, order_id.0,
        ));

        let _ = self.request_tx.send(Request::SubmitOrder {
            symbol: Symbol(f.symbol),
            order,
        });
    }

    fn type_char(&mut self, c: char) {
        match &mut self.screen {
            Screen::NumberInput { buf, next, .. } => {
                // Allow minus sign for fee fields (rebates are negative).
                let is_fee = matches!(
                    next,
                    NextStep::FeeScheduleMakerBps { .. } | NextStep::FeeScheduleTakerBps { .. }
                );
                if c.is_ascii_digit() || (is_fee && c == '-' && buf.is_empty()) {
                    buf.push(c);
                }
            }
            Screen::CommandInput { buf } => {
                buf.push(c);
            }
            _ => {}
        }
    }

    fn backspace(&mut self) {
        match &mut self.screen {
            Screen::NumberInput { buf, .. } | Screen::CommandInput { buf } => {
                buf.pop();
            }
            _ => {}
        }
    }
}

// ── Text command parsing (power-user fallback) ──────────────────────

fn parse_text_command(input: &str) -> Result<Request, String> {
    let parts: Vec<&str> = input.split_whitespace().collect();
    if parts.is_empty() {
        return Err("empty command".into());
    }
    match parts[0] {
        "cancel" if parts.len() >= 4 => {
            let sym: u32 = parts[1].parse().map_err(|_| "invalid symbol")?;
            let acct: u32 = parts[2].parse().map_err(|_| "invalid account")?;
            let oid: u64 = parts[3].parse().map_err(|_| "invalid order_id")?;
            Ok(Request::CancelOrder {
                symbol: Symbol(sym),
                account: AccountId(acct),
                order_id: OrderId(oid),
            })
        }
        "cancel-all" if parts.len() >= 2 => {
            let acct: u32 = parts[1].parse().map_err(|_| "invalid account")?;
            Ok(Request::CancelAll {
                account: AccountId(acct),
            })
        }
        _ => Err(format!(
            "unknown command: {} (try: cancel <sym> <id>, cancel-all <acct>)",
            parts[0]
        )),
    }
}

// ── Response formatting ─────────────────────────────────────────────

fn format_report(report: &ExecutionReport) -> String {
    match *report {
        ExecutionReport::Placed {
            order_id,
            symbol,
            account,
            side,
            price,
            quantity,
        } => {
            let side_str = if side == Side::Buy { "BUY" } else { "SELL" };
            format!(
                "PLACED  #{} sym={} {} @{} x{} acct={}",
                order_id.0, symbol.0, side_str, price.0, quantity.0, account.0,
            )
        }
        ExecutionReport::Fill {
            maker_order_id,
            taker_order_id,
            price,
            quantity,
            maker_fee,
            taker_fee,
            ..
        } => {
            let fee_str = if maker_fee != 0 || taker_fee != 0 {
                format!(" fees:m={maker_fee}/t={taker_fee}")
            } else {
                String::new()
            };
            format!(
                "FILL    maker #{} / taker #{} @{} x{}{}",
                maker_order_id.0, taker_order_id.0, price.0, quantity.0, fee_str,
            )
        }
        ExecutionReport::Cancelled {
            order_id,
            remaining_quantity,
            ..
        } => format!(
            "CANCEL  #{} (remaining: {})",
            order_id.0, remaining_quantity.0,
        ),
        ExecutionReport::Triggered {
            order_id,
            symbol,
            account,
            trigger_price,
        } => format!(
            "TRIGGER #{} sym={} @{} acct={}",
            order_id.0, symbol.0, trigger_price.0, account.0,
        ),
        ExecutionReport::Rejected {
            order_id, reason, ..
        } => {
            let reason_str = match reason {
                RejectReason::NoLiquidity => "no liquidity",
                RejectReason::FOKCannotFill => "FOK cannot fill",
                RejectReason::InsufficientBalance => "insufficient balance",
                RejectReason::UnknownAccount => "unknown account",
                RejectReason::UnknownSymbol => "unknown symbol",
                RejectReason::SelfTradePrevented => "self-trade prevented",
                RejectReason::DuplicateOrderId => "duplicate order ID",
                RejectReason::ExceedsMaxOrderQty => "exceeds max order size",
                RejectReason::ExceedsMaxNotional => "exceeds max notional",
                RejectReason::TradingHalted => "trading halted",
                RejectReason::OutsidePriceBand => "outside price band",
                RejectReason::UnknownOrder => "unknown order",
                RejectReason::PriceWouldCross => "price would cross spread",
                RejectReason::PostOnlyWouldCross => "post-only would cross",
                RejectReason::HasRestingOrders => "has resting orders",
                RejectReason::DuplicateRequest => "duplicate request",
                RejectReason::ReplicaDisconnected => "replica disconnected",
                RejectReason::InvalidExpiry => "invalid expiry",
                RejectReason::InstrumentDisabled => "instrument disabled",
            };
            format!("REJECT  #{} ({reason_str})", order_id.0)
        }
        ExecutionReport::Replaced {
            order_id,
            symbol,
            account,
            side,
            old_price,
            new_price,
            old_remaining,
            new_remaining,
        } => {
            let side_str = if side == Side::Buy { "BUY" } else { "SELL" };
            format!(
                "REPLACE #{} sym={} {} @{}→{} x{}→{} acct={}",
                order_id.0,
                symbol.0,
                side_str,
                old_price.0,
                new_price.0,
                old_remaining.0,
                new_remaining.0,
                account.0,
            )
        }
        ExecutionReport::InstrumentStatusChanged { symbol, status } => {
            let status_str = match status {
                InstrumentStatus::Enabled => "ENABLED",
                InstrumentStatus::Disabled => "DISABLED",
                InstrumentStatus::Removed => "REMOVED",
            };
            format!("INSTRUMENT {} → {}", symbol.0, status_str)
        }
    }
}

// ── Client thread ───────────────────────────────────────────────────

fn client_thread(
    addr: SocketAddr,
    key: &ed25519_dalek::SigningKey,
    request_rx: mpsc::Receiver<Request>,
    response_tx: mpsc::Sender<String>,
    stats_tx: mpsc::Sender<StatsSnapshot>,
) {
    let mut client = match Client::connect(addr, key) {
        Ok(c) => {
            let _ = response_tx.send(format!("Connected to {addr}"));
            c
        }
        Err(e) => {
            let _ = response_tx.send(format!("Connection failed: {e}"));
            return;
        }
    };

    while let Ok(request) = request_rx.recv() {
        let start = std::time::Instant::now();
        match client.send_request(&request) {
            Ok(responses) => {
                let latency = start.elapsed();
                if responses.is_empty() {
                    let _ = response_tx.send(format!("(no reports)  [{latency:.3?}]"));
                }
                for resp in &responses {
                    let msg = match resp {
                        ResponseKind::Report(report) => format_report(report),
                        ResponseKind::EngineError => "ENGINE ERROR".into(),
                        ResponseKind::ServerBusy => "SERVER BUSY (pipeline full)".into(),
                        ResponseKind::BatchEnd
                        | ResponseKind::ServerReady
                        | ResponseKind::Heartbeat
                        | ResponseKind::Challenge { .. }
                        | ResponseKind::AuthFailed
                        | ResponseKind::BookSnapshotBegin { .. }
                        | ResponseKind::BookSnapshotLevel { .. }
                        | ResponseKind::BookSnapshotEnd { .. }
                        | ResponseKind::SnapshotComplete { .. }
                        | ResponseKind::PositionSnapshot { .. } => continue,
                        ResponseKind::StatsHeader {
                            active_connections,
                            events_processed,
                            journal_sequence,
                        } => {
                            // Best-effort: receiver may be gone if UI quit.
                            let _ = stats_tx.send(StatsSnapshot {
                                active_connections: *active_connections,
                                events_processed: *events_processed,
                                journal_sequence: *journal_sequence,
                            });
                            continue;
                        }
                    };
                    let _ = response_tx.send(format!("{msg}  [{latency:.3?}]"));
                }
            }
            Err(e) => {
                let _ = response_tx.send(format!("Request failed: {e}"));
                break;
            }
        }
    }
}

// ── Drawing ─────────────────────────────────────────────────────────

fn draw(frame: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // dashboard
            Constraint::Min(5),    // log
            Constraint::Length(3), // status bar
        ])
        .split(frame.area());

    draw_dashboard(frame, app, chunks[0]);
    draw_log(frame, app, chunks[1]);
    draw_status_bar(frame, app, chunks[2]);

    match &app.screen {
        Screen::ActionMenu => {
            draw_menu(frame, "Action", ACTIONS.iter().copied(), app.cursor);
        }
        Screen::TifMenu { .. } => {
            let items: Vec<&str> = TIF_OPTIONS.iter().map(|(name, _)| *name).collect();
            draw_menu(frame, "Time in Force", items.into_iter(), app.cursor);
        }
        Screen::StpMenu { .. } => {
            let items: Vec<&str> = STP_OPTIONS.iter().map(|(name, _)| *name).collect();
            draw_menu(
                frame,
                "Self-Trade Prevention",
                items.into_iter(),
                app.cursor,
            );
        }
        Screen::NumberInput { label, buf, .. } => {
            draw_input(frame, label, buf);
        }
        Screen::CommandInput { buf } => {
            draw_command_input(frame, buf);
        }
    }
}

fn draw_dashboard(frame: &mut Frame, app: &App, area: Rect) {
    let text = if let Some(stats) = &app.stats {
        let tput = if stats.throughput >= 1_000_000 {
            format!("{:.1}M/s", stats.throughput as f64 / 1_000_000.0)
        } else if stats.throughput >= 1_000 {
            format!("{}K/s", stats.throughput / 1_000)
        } else {
            format!("{}/s", stats.throughput)
        };
        format!(
            " Connections: {}    Events: {}    Throughput: {}    Journal: #{}",
            stats.active_connections, stats.events_processed, tput, stats.journal_sequence,
        )
    } else {
        " Waiting for stats...".into()
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Dashboard ")
        .border_style(Style::default().fg(Color::DarkGray));
    let paragraph = Paragraph::new(text).block(block);
    frame.render_widget(paragraph, area);
}

fn draw_log(frame: &mut Frame, app: &App, area: Rect) {
    let height = area.height.saturating_sub(2) as usize;
    let start = app.log.len().saturating_sub(height);
    let items: Vec<ListItem> = app.log[start..]
        .iter()
        .map(|s| {
            let style = if s.starts_with("FILL") {
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD)
            } else if s.starts_with("REJECT") || s.starts_with("ENGINE") || s.starts_with("ERROR") {
                Style::default().fg(Color::Red)
            } else if s.starts_with("PLACED") {
                Style::default().fg(Color::Cyan)
            } else if s.starts_with("CANCEL") {
                Style::default().fg(Color::Yellow)
            } else if s.starts_with("TRIGGER") {
                Style::default().fg(Color::Magenta)
            } else if s.starts_with("→") || s.starts_with(">") {
                Style::default().fg(Color::DarkGray)
            } else {
                Style::default()
            };
            ListItem::new(Line::from(Span::styled(s.as_str(), style)))
        })
        .collect();

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Admin Console ")
        .title_style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        );
    frame.render_widget(List::new(items).block(block), area);
}

fn draw_status_bar(frame: &mut Frame, app: &App, area: Rect) {
    let breadcrumb = match &app.screen {
        Screen::ActionMenu => "Select action".into(),
        Screen::NumberInput { label, .. } => format!("Enter {label}"),
        Screen::TifMenu { collected } => {
            format!("{} → Time in Force", ACTIONS[collected.action])
        }
        Screen::StpMenu { collected } => {
            format!("{} → Self-Trade Prevention", ACTIONS[collected.action])
        }
        Screen::CommandInput { .. } => "Text command mode".into(),
    };

    let help = " ↑↓ navigate │ Enter select │ Esc back │ Tab text mode ";
    let bar = Paragraph::new(Line::from(vec![
        Span::styled(format!(" {breadcrumb} "), Style::default().fg(Color::White)),
        Span::styled(help, Style::default().fg(Color::DarkGray)),
    ]))
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(bar, area);
}

fn draw_menu<'a>(
    frame: &mut Frame,
    title: &str,
    items: impl Iterator<Item = &'a str>,
    cursor: usize,
) {
    let items_vec: Vec<&str> = items.collect();
    let height = items_vec.len() as u16 + 2;
    let area = centered_rect(40, height, frame.area());

    let menu_items: Vec<ListItem> = items_vec
        .iter()
        .enumerate()
        .map(|(i, label)| {
            let style = if i == cursor {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::White)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::White)
            };
            let width = area.width.saturating_sub(2) as usize;
            ListItem::new(format!(" {label:<width$}")).style(style)
        })
        .collect();

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan))
        .title(format!(" {title} "))
        .title_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );

    frame.render_widget(Clear, area);
    frame.render_widget(List::new(menu_items).block(block), area);
}

fn draw_input(frame: &mut Frame, label: &str, buf: &str) {
    let area = centered_rect(30, 3, frame.area());

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan))
        .title(format!(" {label} "))
        .title_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );

    let input = Paragraph::new(buf)
        .style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )
        .block(block);

    frame.render_widget(Clear, area);
    frame.render_widget(input, area);
    frame.set_cursor_position((area.x + buf.len() as u16 + 1, area.y + 1));
}

fn draw_command_input(frame: &mut Frame, buf: &str) {
    let area = centered_rect(50, 3, frame.area());

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Green))
        .title(" Command (cancel <s> <id> | cancel-all <a>) ")
        .title_style(Style::default().fg(Color::Green));

    let input = Paragraph::new(Line::from(vec![
        Span::styled("> ", Style::default().fg(Color::Green)),
        Span::raw(buf),
    ]))
    .block(block);

    frame.render_widget(Clear, area);
    frame.render_widget(input, area);
    frame.set_cursor_position((area.x + 3 + buf.len() as u16, area.y + 1));
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let x = area.x + area.width.saturating_sub(width) / 2;
    let y = area.y + area.height.saturating_sub(height) / 2;
    Rect::new(x, y, width.min(area.width), height.min(area.height))
}

// ── Key loading ─────────────────────────────────────────────────────

fn load_signing_key(path: &str) -> ed25519_dalek::SigningKey {
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("cannot read key file {path}: {e}"));
    if bytes.len() != 32 {
        panic!(
            "key file must be exactly 32 bytes (raw Ed25519 seed), got {}",
            bytes.len()
        );
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&bytes);
    ed25519_dalek::SigningKey::from_bytes(&seed)
}

// ── Main ────────────────────────────────────────────────────────────

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: melin-admin <addr> <key-file>");
        std::process::exit(1);
    }

    let addr: SocketAddr = args[1].parse()?;
    let key = load_signing_key(&args[2]);

    let (request_tx, request_rx) = mpsc::channel::<Request>();
    let (response_tx, response_rx) = mpsc::channel::<String>();
    let (stats_tx, stats_rx) = mpsc::channel::<StatsSnapshot>();

    std::thread::Builder::new()
        .name("client".into())
        .spawn(move || client_thread(addr, &key, request_rx, response_tx, stats_tx))
        .expect("spawn client thread");

    let mut terminal = ratatui::init();
    let mut app = App::new(request_tx.clone(), response_rx);
    let stats_request_tx = request_tx;

    loop {
        app.poll_responses();

        // Poll stats from client thread. Only use the latest snapshot
        // if multiple arrived (avoids near-zero elapsed denominator).
        let mut latest_stats: Option<StatsSnapshot> = None;
        while let Ok(raw) = stats_rx.try_recv() {
            latest_stats = Some(raw);
        }
        if let Some(raw) = latest_stats {
            let elapsed = app.last_stats_poll.elapsed().as_secs_f64();
            let throughput = if let Some(prev) = &app.stats {
                if elapsed > 0.01 {
                    ((raw.events_processed.saturating_sub(prev.events_processed)) as f64 / elapsed)
                        as u64
                } else {
                    prev.throughput
                }
            } else {
                0
            };
            app.stats = Some(DashboardStats {
                active_connections: raw.active_connections,
                events_processed: raw.events_processed,
                journal_sequence: raw.journal_sequence,
                throughput,
            });
            app.last_stats_poll = Instant::now();
        }

        // Send a QueryStats request every second.
        if app.last_stats_poll.elapsed() >= std::time::Duration::from_secs(1) {
            let _ = stats_request_tx.send(Request::QueryStats);
        }

        terminal.draw(|f| draw(f, &app))?;

        if app.quit {
            break;
        }

        if event::poll(std::time::Duration::from_millis(50))?
            && let Event::Key(key) = event::read()?
        {
            if key.kind != KeyEventKind::Press {
                continue;
            }
            match key.code {
                KeyCode::Up => app.move_up(),
                KeyCode::Down => app.move_down(),
                KeyCode::Enter => app.select(),
                KeyCode::Esc => app.go_back(),
                KeyCode::Tab => {
                    // Toggle between menu and text command mode.
                    match &app.screen {
                        Screen::CommandInput { .. } => {
                            app.screen = Screen::ActionMenu;
                            app.cursor = 0;
                        }
                        Screen::ActionMenu => {
                            app.screen = Screen::CommandInput { buf: String::new() };
                        }
                        _ => {}
                    }
                }
                KeyCode::Char('q') if matches!(app.screen, Screen::ActionMenu) => {
                    app.quit = true;
                }
                KeyCode::Char(c) => app.type_char(c),
                KeyCode::Backspace => app.backspace(),
                _ => {}
            }
        }
    }

    ratatui::restore();
    Ok(())
}
