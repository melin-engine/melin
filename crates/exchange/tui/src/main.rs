/// jemalloc: thread-local caches eliminate allocator lock contention,
/// giving more predictable latency than glibc malloc under high throughput.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::sync::mpsc;

use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph};

use melin_client::Client;
use melin_protocol::message::{Request, ResponseKind};
use melin_protocol::types::{
    AccountId, ExecutionReport, InstrumentStatus, Order, OrderId, OrderType, Price, Quantity,
    RejectReason, SelfTradeProtection, Side, Symbol, TimeInForce,
};

// ── Menu definitions ────────────────────────────────────────────────

/// Top-level actions the user can pick.
const ACTIONS: &[&str] = &[
    "Limit Buy",
    "Limit Sell",
    "Market Buy",
    "Market Sell",
    "Cancel Order",
];

/// Available instruments (matches server seed data).
const SYMBOLS: &[(&str, u32)] = &[("BTC/USD", 1), ("ETH/USD", 2)];

/// Available accounts (matches server seed data).
const ACCOUNTS: &[(&str, u32)] = &[("Account 1", 1), ("Account 2", 2)];

/// Time-in-force options.
const TIF_OPTIONS: &[(&str, u8)] = &[("GTC", 0), ("IOC", 1), ("FOK", 2), ("Day", 3), ("GTD", 4)];

// ── App state ───────────────────────────────────────────────────────

/// Which screen/step the user is on.
enum Screen {
    /// Picking an action from the top-level menu.
    ActionMenu,
    /// Picking an instrument (symbol).
    SymbolMenu { action: usize },
    /// Picking an account.
    AccountMenu { action: usize, symbol: usize },
    /// Picking time-in-force (limit orders only).
    TifMenu {
        action: usize,
        symbol: usize,
        account: usize,
    },
    /// Typing a numeric value (price or quantity).
    NumberInput {
        action: usize,
        symbol: usize,
        account: usize,
        tif: usize,
        /// Which field we're entering.
        field: InputField,
        /// What the user has typed so far.
        buf: String,
        /// For limit orders: the price, once entered.
        price: Option<u64>,
    },
    /// Picking an account for cancel.
    CancelAccountMenu { symbol: usize },
    /// Typing the order ID to cancel.
    CancelInput {
        symbol: usize,
        account: usize,
        buf: String,
    },
}

#[derive(Clone, Copy)]
enum InputField {
    Price,
    Quantity,
}

struct App {
    screen: Screen,
    /// Cursor position in the current menu.
    cursor: usize,
    /// Log of formatted events.
    log: Vec<String>,
    request_tx: mpsc::Sender<Request>,
    response_rx: mpsc::Receiver<String>,
    quit: bool,
    next_order_id: u64,
}

impl App {
    fn new(request_tx: mpsc::Sender<Request>, response_rx: mpsc::Receiver<String>) -> Self {
        Self {
            screen: Screen::ActionMenu,
            cursor: 0,
            log: vec!["Connected. Select an action with ↑/↓ and Enter.".into()],
            request_tx,
            response_rx,
            quit: false,
            next_order_id: 1,
        }
    }

    fn poll_responses(&mut self) {
        while let Ok(msg) = self.response_rx.try_recv() {
            self.log.push(msg);
        }
    }

    /// How many items in the current menu (0 if not on a menu screen).
    fn menu_len(&self) -> usize {
        match &self.screen {
            Screen::ActionMenu => ACTIONS.len(),
            Screen::SymbolMenu { .. } => SYMBOLS.len(),
            Screen::AccountMenu { .. } => ACCOUNTS.len(),
            Screen::TifMenu { .. } => TIF_OPTIONS.len(),
            Screen::CancelAccountMenu { .. } => ACCOUNTS.len(),
            Screen::NumberInput { .. } | Screen::CancelInput { .. } => 0,
        }
    }

    fn move_cursor_up(&mut self) {
        let len = self.menu_len();
        if len > 0 {
            self.cursor = (self.cursor + len - 1) % len;
        }
    }

    fn move_cursor_down(&mut self) {
        let len = self.menu_len();
        if len > 0 {
            self.cursor = (self.cursor + 1) % len;
        }
    }

    /// Go back one step, or quit if already at the top.
    fn go_back(&mut self) {
        self.screen = match &self.screen {
            Screen::ActionMenu => {
                self.quit = true;
                return;
            }
            Screen::SymbolMenu { .. } => Screen::ActionMenu,
            Screen::AccountMenu { action, .. } => Screen::SymbolMenu { action: *action },
            Screen::TifMenu { action, symbol, .. } => Screen::AccountMenu {
                action: *action,
                symbol: *symbol,
            },
            Screen::NumberInput {
                action,
                symbol,
                account,
                tif,
                field,
                price,
                ..
            } => {
                // If entering quantity, go back to price. If entering price, go back to TIF.
                match field {
                    InputField::Quantity if price.is_some() => Screen::NumberInput {
                        action: *action,
                        symbol: *symbol,
                        account: *account,
                        tif: *tif,
                        field: InputField::Price,
                        buf: String::new(),
                        price: None,
                    },
                    _ => Screen::TifMenu {
                        action: *action,
                        symbol: *symbol,
                        account: *account,
                    },
                }
            }
            Screen::CancelAccountMenu { .. } => Screen::SymbolMenu { action: 4 },
            Screen::CancelInput { symbol, .. } => Screen::CancelAccountMenu { symbol: *symbol },
        };
        self.cursor = 0;
    }

    /// Handle Enter press on a menu or input screen.
    fn select(&mut self) {
        match &self.screen {
            Screen::ActionMenu => {
                let action = self.cursor;
                self.screen = Screen::SymbolMenu { action };
                self.cursor = 0;
            }
            Screen::SymbolMenu { action } => {
                let action = *action;
                let symbol = self.cursor;
                if action == 4 {
                    // Cancel → go to account selection.
                    self.screen = Screen::CancelAccountMenu { symbol };
                    self.cursor = 0;
                } else {
                    self.screen = Screen::AccountMenu { action, symbol };
                    self.cursor = 0;
                }
            }
            Screen::AccountMenu { action, symbol } => {
                let action = *action;
                let symbol = *symbol;
                let account = self.cursor;
                if action <= 1 {
                    // Limit order → pick TIF next.
                    self.screen = Screen::TifMenu {
                        action,
                        symbol,
                        account,
                    };
                    self.cursor = 0;
                } else {
                    // Market order → go straight to quantity.
                    self.screen = Screen::NumberInput {
                        action,
                        symbol,
                        account,
                        tif: 0, // GTC default for market
                        field: InputField::Quantity,
                        buf: String::new(),
                        price: None,
                    };
                }
            }
            Screen::TifMenu {
                action,
                symbol,
                account,
            } => {
                let action = *action;
                let symbol = *symbol;
                let account = *account;
                let tif = self.cursor;
                // Limit order → enter price.
                self.screen = Screen::NumberInput {
                    action,
                    symbol,
                    account,
                    tif,
                    field: InputField::Price,
                    buf: String::new(),
                    price: None,
                };
            }
            Screen::NumberInput {
                action,
                symbol,
                account,
                tif,
                field,
                buf,
                price,
            } => {
                let val: u64 = match buf.parse() {
                    Ok(v) if v > 0 => v,
                    _ => {
                        self.log
                            .push("Invalid input (expected positive number).".into());
                        return;
                    }
                };
                let action = *action;
                let symbol_idx = *symbol;
                let account_idx = *account;
                let tif_idx = *tif;

                match field {
                    InputField::Price => {
                        // Price entered, now ask for quantity.
                        self.screen = Screen::NumberInput {
                            action,
                            symbol: symbol_idx,
                            account: account_idx,
                            tif: tif_idx,
                            field: InputField::Quantity,
                            buf: String::new(),
                            price: Some(val),
                        };
                    }
                    InputField::Quantity => {
                        self.submit_order(action, symbol_idx, account_idx, tif_idx, *price, val);
                        self.screen = Screen::ActionMenu;
                        self.cursor = 0;
                    }
                }
            }
            Screen::CancelAccountMenu { symbol } => {
                let symbol = *symbol;
                let account = self.cursor;
                self.screen = Screen::CancelInput {
                    symbol,
                    account,
                    buf: String::new(),
                };
            }
            Screen::CancelInput {
                symbol,
                account,
                buf,
            } => {
                let order_id: u64 = match buf.parse() {
                    Ok(v) => v,
                    _ => {
                        self.log.push("Invalid order ID.".into());
                        return;
                    }
                };
                let sym = Symbol(SYMBOLS[*symbol].1);
                let acc = AccountId(ACCOUNTS[*account].1);
                let request = Request::CancelOrder {
                    symbol: sym,
                    account: acc,
                    order_id: OrderId(order_id),
                };
                self.log.push(format!(
                    "Cancelling order #{order_id} on {} for {}",
                    SYMBOLS[*symbol].0, ACCOUNTS[*account].0
                ));
                if self.request_tx.send(request).is_err() {
                    self.log.push("Disconnected.".into());
                }
                self.screen = Screen::ActionMenu;
                self.cursor = 0;
            }
        }
    }

    fn submit_order(
        &mut self,
        action: usize,
        symbol_idx: usize,
        account_idx: usize,
        tif_idx: usize,
        price: Option<u64>,
        quantity: u64,
    ) {
        let sym = Symbol(SYMBOLS[symbol_idx].1);
        let acc = AccountId(ACCOUNTS[account_idx].1);
        let side = if action.is_multiple_of(2) {
            Side::Buy
        } else {
            Side::Sell
        };
        let tif = match tif_idx {
            1 => TimeInForce::IOC,
            2 => TimeInForce::FOK,
            3 => TimeInForce::Day,
            4 => TimeInForce::GTD,
            _ => TimeInForce::GTC,
        };

        let order_id = OrderId(self.next_order_id);
        self.next_order_id += 1;

        let order_type = if let Some(p) = price {
            OrderType::Limit {
                price: Price(NonZeroU64::new(p).expect("validated > 0")),
                post_only: false,
            }
        } else {
            OrderType::Market
        };

        let qty = Quantity(NonZeroU64::new(quantity).expect("validated > 0"));

        let order = Order {
            id: order_id,
            account: acc,
            side,
            order_type,
            time_in_force: tif,
            quantity: qty,
            stp: SelfTradeProtection::default(),
            expiry_ns: 0,
        };

        let side_str = if side == Side::Buy { "BUY" } else { "SELL" };
        let type_str = match price {
            Some(p) => format!("LIMIT @ {p}"),
            None => "MARKET".into(),
        };
        self.log.push(format!(
            "→ {} {} {} x{} [{}] (order #{})",
            side_str, type_str, SYMBOLS[symbol_idx].0, quantity, TIF_OPTIONS[tif_idx].0, order_id.0,
        ));

        let request = Request::SubmitOrder { symbol: sym, order };
        if self.request_tx.send(request).is_err() {
            self.log.push("Disconnected.".into());
        }
    }

    /// Handle a character typed into a number/cancel input field.
    fn type_char(&mut self, c: char) {
        match &mut self.screen {
            Screen::NumberInput { buf, .. } | Screen::CancelInput { buf, .. }
                if c.is_ascii_digit() =>
            {
                buf.push(c);
            }
            _ => {}
        }
    }

    fn backspace(&mut self) {
        match &mut self.screen {
            Screen::NumberInput { buf, .. } | Screen::CancelInput { buf, .. } => {
                buf.pop();
            }
            _ => {}
        }
    }
}

// ── Formatting ──────────────────────────────────────────────────────

fn format_report(report: &ExecutionReport) -> String {
    match report {
        ExecutionReport::Placed {
            order_id,
            symbol,
            account,
            side,
            price,
            quantity,
        } => {
            let side_str = if *side == Side::Buy { "BUY" } else { "SELL" };
            format!(
                "PLACED  #{} sym={} {} @ {} x{} acct={}",
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
            let fee_str = if *maker_fee != 0 || *taker_fee != 0 {
                format!(" fees:m={maker_fee}/t={taker_fee}")
            } else {
                String::new()
            };
            format!(
                "FILL    maker #{} / taker #{} @ {} x{}{}",
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
            "TRIGGER #{} sym={} @ {} acct={}",
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
                RejectReason::ExceedsMaxOpenOrders => "exceeds max open orders",
                RejectReason::ExceedsOrderRate => "exceeds order rate limit",
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
            let side_str = if *side == Side::Buy { "BUY" } else { "SELL" };
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
                        | ResponseKind::StatsHeader { .. }
                        | ResponseKind::BookSnapshotBegin { .. }
                        | ResponseKind::BookSnapshotLevel { .. }
                        | ResponseKind::BookSnapshotEnd { .. }
                        | ResponseKind::SnapshotComplete { .. }
                        | ResponseKind::PositionSnapshot { .. }
                        | ResponseKind::RequestSeqHwm { .. } => continue,
                    };
                    let _ = response_tx.send(msg);
                }
                if responses.is_empty() {
                    let _ = response_tx.send("(no reports)".into());
                }
                let _ = response_tx.send(format!("  ⏱ {latency:.3?}"));
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
            Constraint::Min(5),    // log area
            Constraint::Length(3), // status bar
        ])
        .split(frame.area());

    draw_log(frame, app, chunks[0]);
    draw_status_bar(frame, app, chunks[1]);

    // Draw the menu/input overlay in the center.
    match &app.screen {
        Screen::ActionMenu => {
            draw_menu(frame, "Action", ACTIONS.iter().copied(), app.cursor);
        }
        Screen::SymbolMenu { .. } => {
            let items: Vec<&str> = SYMBOLS.iter().map(|(name, _)| *name).collect();
            draw_menu(frame, "Instrument", items.into_iter(), app.cursor);
        }
        Screen::AccountMenu { .. } => {
            let items: Vec<&str> = ACCOUNTS.iter().map(|(name, _)| *name).collect();
            draw_menu(frame, "Account", items.into_iter(), app.cursor);
        }
        Screen::TifMenu { .. } => {
            let items: Vec<&str> = TIF_OPTIONS.iter().map(|(name, _)| *name).collect();
            draw_menu(frame, "Time in Force", items.into_iter(), app.cursor);
        }
        Screen::NumberInput { field, buf, .. } => {
            let label = match field {
                InputField::Price => "Price",
                InputField::Quantity => "Quantity",
            };
            draw_input(frame, label, buf);
        }
        Screen::CancelAccountMenu { .. } => {
            let items: Vec<&str> = ACCOUNTS.iter().map(|(name, _)| *name).collect();
            draw_menu(frame, "Account", items.into_iter(), app.cursor);
        }
        Screen::CancelInput { buf, .. } => {
            draw_input(frame, "Order ID", buf);
        }
    }
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
            } else if s.starts_with("REJECT") || s.starts_with("ENGINE ERROR") {
                Style::default().fg(Color::Red)
            } else if s.starts_with("PLACED") {
                Style::default().fg(Color::Cyan)
            } else if s.starts_with("CANCEL") {
                Style::default().fg(Color::Yellow)
            } else if s.starts_with("TRIGGER") {
                Style::default().fg(Color::Magenta)
            } else if s.starts_with('→') {
                Style::default().fg(Color::DarkGray)
            } else {
                Style::default()
            };
            ListItem::new(Line::from(Span::styled(s.as_str(), style)))
        })
        .collect();

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Trading TUI ")
        .title_style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        );
    let list = List::new(items).block(block);
    frame.render_widget(list, area);
}

fn draw_status_bar(frame: &mut Frame, app: &App, area: Rect) {
    let breadcrumb = match &app.screen {
        Screen::ActionMenu => "Select action".into(),
        Screen::SymbolMenu { action } => format!("{} → Select instrument", ACTIONS[*action]),
        Screen::AccountMenu { action, symbol } => {
            format!(
                "{} → {} → Select account",
                ACTIONS[*action], SYMBOLS[*symbol].0,
            )
        }
        Screen::TifMenu { action, symbol, .. } => {
            format!(
                "{} → {} → Select time-in-force",
                ACTIONS[*action], SYMBOLS[*symbol].0,
            )
        }
        Screen::NumberInput {
            action,
            symbol,
            field,
            ..
        } => {
            let field_str = match field {
                InputField::Price => "Enter price",
                InputField::Quantity => "Enter quantity",
            };
            format!(
                "{} → {} → {field_str}",
                ACTIONS[*action], SYMBOLS[*symbol].0
            )
        }
        Screen::CancelAccountMenu { symbol } => {
            format!("Cancel → {} → Select account", SYMBOLS[*symbol].0)
        }
        Screen::CancelInput { symbol, .. } => {
            format!("Cancel → {} → Enter order ID", SYMBOLS[*symbol].0)
        }
    };

    let help = " ↑↓ navigate │ Enter select │ Esc back │ q quit ";
    let bar = Paragraph::new(Line::from(vec![
        Span::styled(format!(" {breadcrumb} "), Style::default().fg(Color::White)),
        Span::styled(help, Style::default().fg(Color::DarkGray)),
    ]))
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(bar, area);
}

/// Draw a centered menu overlay.
fn draw_menu<'a>(
    frame: &mut Frame,
    title: &str,
    items: impl Iterator<Item = &'a str>,
    cursor: usize,
) {
    let area = centered_rect(30, items.size_hint().0 as u16 + 2, frame.area());

    // We need to collect since we consumed the size hint.
    let items_vec: Vec<&str> = items.collect();
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
            // Pad to fill the width so the highlight spans the row.
            let width = area.width.saturating_sub(2) as usize;
            let padded = format!(" {label:<width$}");
            ListItem::new(padded).style(style)
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
    let list = List::new(menu_items).block(block);
    frame.render_widget(list, area);
}

/// Draw a centered text input overlay.
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

    // Cursor.
    frame.set_cursor_position((area.x + buf.len() as u16 + 1, area.y + 1));
}

/// Return a centered rectangle of the given width and height.
fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let x = area.x + area.width.saturating_sub(width) / 2;
    let y = area.y + area.height.saturating_sub(height) / 2;
    Rect::new(x, y, width.min(area.width), height.min(area.height))
}

// ── Main ────────────────────────────────────────────────────────────

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9876".into())
        .parse()?;

    let key_path = std::env::args()
        .nth(2)
        .unwrap_or_else(|| panic!("usage: trading-tui <addr> <key-file>"));
    let key = load_signing_key(&key_path);

    let (request_tx, request_rx) = mpsc::channel::<Request>();
    let (response_tx, response_rx) = mpsc::channel::<String>();

    std::thread::Builder::new()
        .name("client".into())
        .spawn(move || client_thread(addr, &key, request_rx, response_tx))
        .expect("spawn client thread");

    let mut terminal = ratatui::init();
    let mut app = App::new(request_tx, response_rx);

    loop {
        app.poll_responses();
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
                KeyCode::Up => app.move_cursor_up(),
                KeyCode::Down => app.move_cursor_down(),
                KeyCode::Enter => app.select(),
                KeyCode::Esc => app.go_back(),
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
