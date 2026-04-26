//! TUI trading client that speaks FIX 4.4 to both the oe-gateway and md-gateway.
//!
//! Usage:
//!   melin-tui-fix-client --oe-addr 127.0.0.1:9000 --md-addr 127.0.0.1:9001 \
//!     --sender CLIENT --oe-target MELIN-OE --md-target MELIN-MD

mod bot;
pub mod fix_client;

use std::io;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use fix_client::FixClient;
use melin_gateway_core::fix::parse::{Field, FixMessage};
use melin_gateway_core::fix::serialize::FixMessageBuilder;
use melin_gateway_core::fix::tags;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Paragraph, Row, Table};

/// (price, size, order_count) for one book level.
type BookLevel = (String, String, String);

// --- Messages between threads ------------------------------------------------

enum UiMsg {
    MdStatus(bool, String),
    Book(Vec<BookLevel>, Vec<BookLevel>),
    OeStatus(bool, String),
    ActiveOrders(Vec<String>),
    Balances(Vec<String>),
    OrderAck(String),
    Log(String),
}

/// Commands sent from the render thread to the OE session thread.
enum OrderCmd {
    NewOrder {
        symbol: String,
        side: String,
        price: String,
        qty: String,
        clord_id: String,
    },
}

// --- Order entry form --------------------------------------------------------

/// Which field in the order form is focused.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FormField {
    Symbol,
    Side,
    Price,
    Qty,
}

impl FormField {
    fn next(self) -> Self {
        match self {
            Self::Symbol => Self::Side,
            Self::Side => Self::Price,
            Self::Price => Self::Qty,
            Self::Qty => Self::Symbol,
        }
    }
    fn prev(self) -> Self {
        match self {
            Self::Symbol => Self::Qty,
            Self::Side => Self::Symbol,
            Self::Price => Self::Side,
            Self::Qty => Self::Price,
        }
    }
}

struct OrderForm {
    symbol: String,
    side: String, // "BUY" or "SELL"
    price: String,
    qty: String,
    focus: FormField,
    active: bool, // whether the form is focused (vs the main view)
    next_clord_id: u64,
    last_ack: String,
}

impl OrderForm {
    fn new() -> Self {
        Self {
            symbol: "BTC/USD".into(),
            side: "BUY".into(),
            price: String::new(),
            qty: String::new(),
            focus: FormField::Price,
            active: false,
            next_clord_id: 1,
            last_ack: String::new(),
        }
    }

    fn toggle_side(&mut self) {
        self.side = if self.side == "BUY" {
            "SELL".into()
        } else {
            "BUY".into()
        };
    }

    fn focused_field_mut(&mut self) -> Option<&mut String> {
        match self.focus {
            FormField::Symbol => Some(&mut self.symbol),
            FormField::Price => Some(&mut self.price),
            FormField::Qty => Some(&mut self.qty),
            FormField::Side => None, // side is toggled, not typed
        }
    }
}

// --- App state ---------------------------------------------------------------

struct App {
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
    active_orders: Vec<String>,
    balances: Vec<String>,
    /// Static keybind/connection banner shown when no session is in error.
    /// The displayed status bar prefers an active error over this banner —
    /// see `App::status_line`.
    banner: String,
    /// Last MD-side error message, cleared when the MD session reports
    /// healthy again. `Option` so a recovered session reverts to the
    /// banner without the caller having to re-send an empty string.
    md_err: Option<String>,
    /// Last OE-side error message, same semantics as `md_err`.
    oe_err: Option<String>,
    md_ok: bool,
    oe_ok: bool,
    form: OrderForm,
    /// Rolling log of recent events (newest at the end, capped at 50).
    logs: Vec<String>,
    /// Optional file for persistent logging.
    log_file: Option<std::fs::File>,
}

impl App {
    fn new(banner: String) -> Self {
        Self {
            bids: vec![],
            asks: vec![],
            active_orders: vec![],
            balances: vec![],
            banner,
            md_err: None,
            oe_err: None,
            md_ok: false,
            oe_ok: false,
            form: OrderForm::new(),
            logs: vec![],
            log_file: std::fs::File::create("tui.log").ok(),
        }
    }
    fn log(&mut self, msg: String) {
        if let Some(f) = &mut self.log_file {
            use std::io::Write;
            let _ = writeln!(f, "{msg}");
        }
        self.logs.push(msg);
        if self.logs.len() > 50 {
            self.logs.remove(0);
        }
    }
    /// Compose the status-bar line from current error state, falling back
    /// to the banner. OE errors are shown ahead of MD errors when both
    /// are active because order entry blocks user action — losing market
    /// data is informational; losing OE means the user can't trade.
    fn status_line(&self) -> &str {
        if let Some(e) = self.oe_err.as_deref() {
            return e;
        }
        if let Some(e) = self.md_err.as_deref() {
            return e;
        }
        &self.banner
    }
    fn drain(&mut self, rx: &Receiver<UiMsg>) {
        while let Ok(m) = rx.try_recv() {
            match m {
                UiMsg::MdStatus(ok, s) => {
                    self.md_ok = ok;
                    self.md_err = if ok { None } else { Some(s) };
                }
                UiMsg::Book(b, a) => {
                    self.bids = b;
                    self.asks = a;
                }
                UiMsg::OeStatus(ok, s) => {
                    self.oe_ok = ok;
                    self.oe_err = if ok { None } else { Some(s) };
                }
                UiMsg::ActiveOrders(o) => self.active_orders = o,
                UiMsg::Balances(b) => self.balances = b,
                UiMsg::OrderAck(s) => {
                    self.log(format!("[OE] {s}"));
                    self.form.last_ack = s;
                }
                UiMsg::Log(s) => self.log(s),
            }
        }
    }
}

// --- Main --------------------------------------------------------------------

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let (mut oe_addr, mut md_addr) = ("127.0.0.1:9000".into(), "127.0.0.1:9001".into());
    let (mut sender, mut oe_target, mut md_target) =
        ("CLIENT".into(), "MELIN-OE".into(), "MELIN-MD".into());
    // When set, spawn a synthetic order-flow bot on a separate FIX session.
    // See `run_bot_session` for the sine-wave rate model.
    let mut bot = false;
    // FIX SenderCompID the bot logs in as. Must be registered as a
    // separate `[[session]]` in the oe-gateway config so its Melin key
    // (and therefore its request-seq namespace) is distinct from the
    // human trader's.
    let mut bot_sender: String = "BOT".into();
    let mut i = 1;
    while i < args.len() {
        let val = || args.get(i + 1).cloned().unwrap_or_default();
        match args[i].as_str() {
            "--oe-addr" => {
                oe_addr = val();
                i += 1;
            }
            "--md-addr" => {
                md_addr = val();
                i += 1;
            }
            "--sender" => {
                sender = val();
                i += 1;
            }
            "--oe-target" => {
                oe_target = val();
                i += 1;
            }
            "--md-target" => {
                md_target = val();
                i += 1;
            }
            "--bot" => {
                bot = true;
            }
            "--bot-sender" => {
                bot_sender = val();
                i += 1;
            }
            _ => {
                eprintln!(
                    "usage: melin-tui-fix-client [--oe-addr ADDR] [--md-addr ADDR] [--sender ID] [--oe-target ID] [--md-target ID] [--bot] [--bot-sender ID]"
                );
                std::process::exit(1);
            }
        }
        i += 1;
    }

    let (tx, rx) = mpsc::channel::<UiMsg>();
    let (order_tx, order_rx) = mpsc::channel::<OrderCmd>();

    // MD session thread.
    let md_tx = tx.clone();
    let (md_a, md_t, md_s) = (md_addr.clone(), md_target.clone(), sender.clone());
    thread::spawn(move || run_md_session(&md_a, &md_s, &md_t, &md_tx));

    // OE session thread.
    let oe_tx = tx.clone();
    let (oe_a, oe_t, oe_s) = (oe_addr.clone(), oe_target.clone(), sender.clone());
    thread::spawn(move || run_oe_session(&oe_a, &oe_s, &oe_t, &oe_tx, &order_rx));

    // Optional bot thread on its own FIX session.
    if bot {
        let bot_tx = tx;
        let (bot_a, bot_t, bot_s) = (oe_addr.clone(), oe_target.clone(), bot_sender.clone());
        thread::spawn(move || run_bot_session(&bot_a, &bot_s, &bot_t, &bot_tx));
    } else {
        // The cloned `tx` above would otherwise leak as a dangling sender.
        drop(tx);
    }

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout))?;
    let mut app = App::new(format!(
        "OE: {oe_addr} -> {oe_target}  |  MD: {md_addr} -> {md_target}  |  'o' order  'q' quit"
    ));
    // Direct log from the render thread — confirms the loop is running and
    // the log panel is functional, independent of any channel messages.
    app.log(format!("[TUI] started — oe={oe_addr} md={md_addr}"));

    loop {
        app.drain(&rx);
        terminal.draw(|f| render(&app, f))?;

        if !event::poll(Duration::from_millis(100))? {
            continue;
        }
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }
        if app.form.active {
            handle_form_key(key.code, &mut app.form, &order_tx);
        } else {
            match key.code {
                KeyCode::Char('q') => break,
                KeyCode::Char('o') => app.form.active = true,
                _ => {}
            }
        }
    }
    disable_raw_mode()?;
    crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

fn handle_form_key(code: KeyCode, form: &mut OrderForm, order_tx: &Sender<OrderCmd>) {
    match code {
        KeyCode::Esc => form.active = false,
        KeyCode::Tab => form.focus = form.focus.next(),
        KeyCode::BackTab => form.focus = form.focus.prev(),
        KeyCode::Enter => {
            if form.price.is_empty() || form.qty.is_empty() || form.symbol.is_empty() {
                return;
            }
            let clord_id = format!("ORD{}", form.next_clord_id);
            form.next_clord_id += 1;
            let _ = order_tx.send(OrderCmd::NewOrder {
                symbol: form.symbol.clone(),
                side: if form.side == "BUY" {
                    "1".into()
                } else {
                    "2".into()
                },
                price: form.price.clone(),
                qty: form.qty.clone(),
                clord_id,
            });
            form.price.clear();
            form.qty.clear();
            form.active = false;
        }
        KeyCode::Char(' ') if form.focus == FormField::Side => form.toggle_side(),
        KeyCode::Char(c) => {
            if let Some(field) = form.focused_field_mut() {
                field.push(c);
            }
        }
        KeyCode::Backspace => {
            if let Some(field) = form.focused_field_mut() {
                field.pop();
            }
        }
        _ => {}
    }
}

// --- Rendering ---------------------------------------------------------------

fn render(app: &App, f: &mut ratatui::Frame<'_>) {
    let v = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(5),    // Top row
            Constraint::Min(5),    // Bottom row
            Constraint::Length(8), // Log panel
            Constraint::Length(2), // Status bar (2 rows: border + content)
        ])
        .split(f.area());

    let top = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(v[0]);
    let bot = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(v[1]);

    let (mi, oi) = (icon(app.md_ok), icon(app.oe_ok));

    // Book.
    render_book(app, f, top[0], mi);

    f.render_widget(
        panel(&format!(" Balances {oi} "), &app.balances, "(waiting)"),
        top[1],
    );
    f.render_widget(
        panel(
            &format!(" Active Orders {oi} "),
            &app.active_orders,
            "(none)",
        ),
        bot[0],
    );
    render_order_form(&app.form, f, bot[1]);

    // Log panel — show last N lines that fit.
    let log_height = v[2].height.saturating_sub(2) as usize; // minus borders
    let skip = app.logs.len().saturating_sub(log_height);
    let log_lines: Vec<Line> = app.logs[skip..]
        .iter()
        .map(|s| {
            Line::from(ratatui::text::Span::styled(
                s.as_str(),
                Style::default().fg(Color::DarkGray),
            ))
        })
        .collect();
    f.render_widget(
        Paragraph::new(log_lines).block(Block::default().title(" Log ").borders(Borders::ALL)),
        v[2],
    );

    f.render_widget(
        Paragraph::new(Line::from(app.status_line()))
            .style(Style::default().fg(Color::Cyan))
            .block(Block::default().borders(Borders::TOP)),
        v[3],
    );
}

fn render_book(app: &App, f: &mut ratatui::Frame<'_>, area: ratatui::layout::Rect, mi: &str) {
    // Book: asks reversed (best at bottom), separator, bids.
    let mut rows: Vec<Row> = Vec::new();
    for a in app.asks.iter().rev() {
        rows.push(
            Row::new(vec!["", "", "", a.0.as_str(), a.1.as_str(), a.2.as_str()])
                .style(Style::default().fg(Color::Red)),
        );
    }
    rows.push(Row::new(vec!["---", "", "", "---", "", ""]));
    for b in &app.bids {
        rows.push(
            Row::new(vec![b.0.as_str(), b.1.as_str(), b.2.as_str(), "", "", ""])
                .style(Style::default().fg(Color::Green)),
        );
    }
    let widths = [
        Constraint::Percentage(20),
        Constraint::Percentage(15),
        Constraint::Percentage(10),
        Constraint::Percentage(20),
        Constraint::Percentage(15),
        Constraint::Percentage(10),
    ];
    f.render_widget(
        Table::new(rows, widths)
            .header(
                Row::new(vec!["Bid Px", "Size", "#", "Ask Px", "Size", "#"])
                    .style(Style::default().add_modifier(Modifier::BOLD)),
            )
            .block(
                Block::default()
                    .title(format!(" Order Book {mi} "))
                    .borders(Borders::ALL),
            ),
        area,
    );
}

fn render_order_form(form: &OrderForm, f: &mut ratatui::Frame<'_>, area: ratatui::layout::Rect) {
    let highlight = if form.active {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let field_style = |ff: FormField| -> Style {
        if form.active && form.focus == ff {
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD)
                .add_modifier(Modifier::UNDERLINED)
        } else {
            highlight
        }
    };

    let side_color = if form.side == "BUY" {
        Color::Green
    } else {
        Color::Red
    };

    let lines = vec![
        Line::from(vec![
            ratatui::text::Span::styled("  Symbol: ", highlight),
            ratatui::text::Span::styled(&form.symbol, field_style(FormField::Symbol)),
        ]),
        Line::from(vec![
            ratatui::text::Span::styled("  Side:   ", highlight),
            ratatui::text::Span::styled(
                &form.side,
                if form.active && form.focus == FormField::Side {
                    Style::default()
                        .fg(side_color)
                        .add_modifier(Modifier::BOLD)
                        .add_modifier(Modifier::UNDERLINED)
                } else {
                    Style::default().fg(side_color)
                },
            ),
            ratatui::text::Span::styled(
                if form.active && form.focus == FormField::Side {
                    " (space)"
                } else {
                    ""
                },
                Style::default().fg(Color::DarkGray),
            ),
        ]),
        Line::from(vec![
            ratatui::text::Span::styled("  Price:  ", highlight),
            ratatui::text::Span::styled(&form.price, field_style(FormField::Price)),
            ratatui::text::Span::styled(
                if form.active && form.focus == FormField::Price {
                    "▌"
                } else {
                    ""
                },
                Style::default().fg(Color::White),
            ),
        ]),
        Line::from(vec![
            ratatui::text::Span::styled("  Qty:    ", highlight),
            ratatui::text::Span::styled(&form.qty, field_style(FormField::Qty)),
            ratatui::text::Span::styled(
                if form.active && form.focus == FormField::Qty {
                    "▌"
                } else {
                    ""
                },
                Style::default().fg(Color::White),
            ),
        ]),
        Line::from(""),
        Line::from(ratatui::text::Span::styled(
            if form.active {
                "  Enter=send  Tab=next  Esc=cancel"
            } else {
                "  Press 'o' to open order form"
            },
            Style::default().fg(Color::DarkGray),
        )),
        if !form.last_ack.is_empty() {
            Line::from(ratatui::text::Span::styled(
                format!("  {}", form.last_ack),
                Style::default().fg(Color::Cyan),
            ))
        } else {
            Line::from("")
        },
    ];

    let border_style = if form.active {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default()
    };

    f.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .title(" Order Entry ")
                .borders(Borders::ALL)
                .border_style(border_style),
        ),
        area,
    );
}

fn icon(ok: bool) -> &'static str {
    if ok { "[+]" } else { "[-]" }
}

fn panel<'a>(title: &'a str, items: &'a [String], empty: &'a str) -> Paragraph<'a> {
    let lines: Vec<Line> = if items.is_empty() {
        vec![Line::from(format!("  {empty}"))]
    } else {
        items.iter().map(|s| Line::from(format!("  {s}"))).collect()
    };
    Paragraph::new(lines).block(
        Block::default()
            .title(title.to_string())
            .borders(Borders::ALL),
    )
}

// --- Reconnect helpers --------------------------------------------------------

/// Capped exponential backoff with jitter. Used by all three sessions when
/// the gateway connection drops so they don't tight-loop reconnecting and
/// don't all hammer the gateway in lockstep on a shared outage.
///
/// Schedule: 100 ms · 2^attempt, capped at 5 s, ±100 ms jitter. The cap is
/// reached at attempt 6 (6 400 ms → clamped to 5 000 ms).
fn backoff_delay(attempt: u32) -> Duration {
    let base_ms = 100u64.saturating_mul(1u64 << attempt.min(6)).min(5_000);
    // Sub-millisecond wall-clock noise → cheap jitter source. Avoids a
    // PRNG dependency for what is purely tie-breaking between threads.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let jitter = (now % 200) as i64 - 100; // -100..=+99 ms
    let total = (base_ms as i64 + jitter).max(50) as u64;
    Duration::from_millis(total)
}

// --- MD session ---------------------------------------------------------------

/// MD session entry point. Wraps `run_md_session_once` in a reconnect loop
/// so a transient gateway disconnect (or a startup-time race) recovers
/// without restarting the TUI.
fn run_md_session(addr: &str, sender: &str, target: &str, tx: &Sender<UiMsg>) {
    let mut attempt: u32 = 0;
    loop {
        match run_md_session_once(addr, sender, target, tx, &mut attempt) {
            Ok(()) => return, // graceful shutdown — channel closed
            Err(e) => {
                let _ = tx.send(UiMsg::MdStatus(false, format!("MD: {e}")));
                let _ = tx.send(UiMsg::Book(vec![], vec![]));
                let delay = backoff_delay(attempt);
                let _ = tx.send(UiMsg::Log(format!(
                    "[MD] disconnected ({e}); reconnecting in {} ms (attempt {})",
                    delay.as_millis(),
                    attempt + 1,
                )));
                thread::sleep(delay);
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

/// One MD session lifetime: connect, list securities, subscribe, snapshot loop.
/// Returns `Ok(())` only on graceful shutdown (UI channel closed); any I/O
/// error bubbles up to the reconnect loop. `attempt` is reset to 0 once
/// init has completed, so a long-stable session that drops once doesn't
/// inherit a high backoff from earlier startup retries.
fn run_md_session_once(
    addr: &str,
    sender: &str,
    target: &str,
    tx: &Sender<UiMsg>,
    attempt: &mut u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tx.send(UiMsg::Log(format!("[MD] connecting to {addr}…")));
    let mut c = FixClient::connect(addr, sender, target, 30)?;
    let _ = tx.send(UiMsg::MdStatus(true, String::new()));
    let _ = tx.send(UiMsg::Log(
        "[MD] connected, sending SecurityListRequest".into(),
    ));

    // Discover symbols via SecurityListRequest.
    let slr = FixMessageBuilder::new(tags::MSG_SECURITY_LIST_REQUEST)
        .str_tag(tags::SECURITY_REQ_ID, "SLR1")
        .str_tag(tags::SECURITY_LIST_REQUEST_TYPE, "0");
    c.send_builder(slr)?;

    let symbols: Vec<String> = match c.recv()? {
        msg if msg.msg_type() == tags::MSG_SECURITY_LIST => {
            let syms: Vec<String> = msg
                .fields_iter()
                .filter(|f| f.tag == tags::SYMBOL)
                .filter_map(|f| std::str::from_utf8(f.value).ok())
                .map(String::from)
                .collect();
            let _ = tx.send(UiMsg::Log(format!("[MD] symbols: {}", syms.join(", "))));
            syms
        }
        msg => {
            let mt = std::str::from_utf8(msg.msg_type()).unwrap_or("?");
            let _ = tx.send(UiMsg::Log(format!(
                "[MD] unexpected response to SLR: 35={mt}"
            )));
            vec![]
        }
    };

    c.set_read_timeout(Some(Duration::from_millis(100)))?;
    // Init complete — clear the backoff so the next disconnect (likely
    // hours from now) starts at the 100 ms floor, not at the cap reached
    // during initial startup retries.
    *attempt = 0;

    // Periodically re-request snapshots (every 1s) since incremental
    // updates (X) aren't wired yet. This ensures the book display
    // reflects orders placed after the initial snapshot.
    let mut last_request = Instant::now() - Duration::from_secs(10);
    let mut req_n: u64 = 0;
    loop {
        if last_request.elapsed() >= Duration::from_secs(1) {
            // Subscribe to the first discovered symbol only.
            if let Some(sym) = symbols.first() {
                req_n += 1;
                let mdr = FixMessageBuilder::new(tags::MSG_MARKET_DATA_REQUEST)
                    .str_tag(tags::MD_REQ_ID, &format!("MD{req_n}"))
                    .str_tag(tags::SUBSCRIPTION_REQUEST_TYPE, "0") // Snapshot
                    .str_tag(tags::MARKET_DEPTH, "0")
                    .u64_tag(tags::NO_RELATED_SYM, 1)
                    .str_tag(tags::SYMBOL, sym);
                c.send_builder(mdr)?;
            }
            last_request = Instant::now();
        }
        c.maintain_heartbeat()?;
        if let Some(msg) = c.try_recv()? {
            let mt_str = std::str::from_utf8(msg.msg_type()).unwrap_or("?");
            let _ = tx.send(UiMsg::Log(format!(
                "[MD<] 35={mt_str} 262={} entries={}",
                msg.get_str(tags::MD_REQ_ID).unwrap_or("-"),
                msg.get_str(tags::NO_MD_ENTRIES).unwrap_or("-"),
            )));
            if msg.msg_type() == tags::MSG_MD_SNAPSHOT {
                let (b, a) = parse_snapshot(&msg);
                if tx.send(UiMsg::Book(b, a)).is_err() {
                    return Ok(()); // UI thread gone — graceful exit
                }
            }
        }
    }
}

/// Extract bid/ask levels from a W (snapshot) message.
fn parse_snapshot(msg: &FixMessage<'_>) -> (Vec<BookLevel>, Vec<BookLevel>) {
    let (mut bids, mut asks) = (Vec::new(), Vec::new());
    let fields: Vec<&Field<'_>> = msg.fields_iter().collect();
    let mut i = 0;
    while i < fields.len() {
        if fields[i].tag == tags::MD_ENTRY_TYPE {
            let et = fields[i].value;
            let val = |off, tag: u32| {
                fields
                    .get(i + off)
                    .filter(|f: &&&Field<'_>| f.tag == tag)
                    .and_then(|f| std::str::from_utf8(f.value).ok())
                    .unwrap_or("-")
                    .to_string()
            };
            let lev = (
                val(1, tags::MD_ENTRY_PX),
                val(2, tags::MD_ENTRY_SIZE),
                val(3, tags::NUMBER_OF_ORDERS),
            );
            match et {
                b"0" => bids.push(lev),
                b"1" => asks.push(lev),
                _ => {}
            }
            i += 4;
        } else {
            i += 1;
        }
    }
    (bids, asks)
}

// --- OE session ---------------------------------------------------------------

/// Local order state maintained from execution reports.
struct LocalOrder {
    order_id: String,
    clord_id: String,
    symbol: String,
    side: &'static str,
    qty: String,
    price: String,
    leaves_qty: String,
}

impl LocalOrder {
    fn display(&self) -> String {
        format!(
            "{} {} {} {}@{} leaves={} ({})",
            self.order_id,
            self.symbol,
            self.side,
            self.qty,
            self.price,
            self.leaves_qty,
            self.clord_id
        )
    }
}

/// Ordered map of active orders, keyed by OrderID.
/// BTreeMap keeps the display order stable.
type OrderTable = std::collections::BTreeMap<String, LocalOrder>;

fn send_order_table(table: &OrderTable, tx: &Sender<UiMsg>) {
    let lines: Vec<String> = table.values().map(|o| o.display()).collect();
    let _ = tx.send(UiMsg::ActiveOrders(lines));
}

/// OE session entry point. Wraps `run_oe_session_once` in a reconnect loop
/// and, while disconnected, drains pending `OrderCmd`s with a "disconnected
/// — order dropped" ack so they don't fire all at once on reconnect.
fn run_oe_session(
    addr: &str,
    sender: &str,
    target: &str,
    tx: &Sender<UiMsg>,
    order_rx: &Receiver<OrderCmd>,
) {
    let mut attempt: u32 = 0;
    loop {
        match run_oe_session_once(addr, sender, target, tx, order_rx, &mut attempt) {
            Ok(()) => return, // graceful — UI gone
            Err(e) => {
                let _ = tx.send(UiMsg::OeStatus(false, format!("OE: {e}")));
                let _ = tx.send(UiMsg::Balances(vec![]));
                let _ = tx.send(UiMsg::ActiveOrders(vec![]));
                let delay = backoff_delay(attempt);
                let _ = tx.send(UiMsg::Log(format!(
                    "[OE] disconnected ({e}); reconnecting in {} ms (attempt {})",
                    delay.as_millis(),
                    attempt + 1,
                )));
                drain_orders_with_reject(order_rx, tx, delay);
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

/// While the OE is reconnecting, reject queued user orders rather than
/// letting them pile up. Without this they would all fire at once when the
/// new session is established, surprising the trader.
fn drain_orders_with_reject(order_rx: &Receiver<OrderCmd>, tx: &Sender<UiMsg>, total: Duration) {
    let deadline = Instant::now() + total;
    while Instant::now() < deadline {
        while let Ok(OrderCmd::NewOrder { clord_id, .. }) = order_rx.try_recv() {
            let _ = tx.send(UiMsg::OrderAck(format!(
                "{clord_id}: REJECTED — OE disconnected"
            )));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        thread::sleep(remaining.min(Duration::from_millis(50)));
    }
}

/// One OE session lifetime: connect, mass-status + positions sync, then
/// the request/response main loop. `attempt` is reset to 0 once init
/// has completed, so a long-stable session that drops once doesn't
/// inherit a high backoff from earlier startup retries.
fn run_oe_session_once(
    addr: &str,
    sender: &str,
    target: &str,
    tx: &Sender<UiMsg>,
    order_rx: &Receiver<OrderCmd>,
    attempt: &mut u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tx.send(UiMsg::Log(format!("[OE] connecting to {addr}…")));
    let mut c = FixClient::connect(addr, sender, target, 30)?;
    let _ = tx.send(UiMsg::OeStatus(true, String::new()));
    let _ = tx.send(UiMsg::Log("[OE] connected".into()));

    // --- One-time sync: mass status + positions ---

    let msr = FixMessageBuilder::new(tags::MSG_ORDER_MASS_STATUS_REQUEST)
        .str_tag(tags::MASS_STATUS_REQ_ID, "INIT")
        .str_tag(tags::MASS_STATUS_REQ_TYPE, "1");
    c.send_builder(msr)?;
    let pr = FixMessageBuilder::new(tags::MSG_REQUEST_FOR_POSITIONS)
        .str_tag(tags::POS_REQ_ID, "INIT")
        .str_tag(tags::POS_REQ_TYPE, "0")
        .str_tag(tags::ACCOUNT, "1");
    c.send_builder(pr)?;

    c.set_read_timeout(Some(Duration::from_millis(100)))?;

    // Drain the initial mass status + position responses.
    let mut orders = OrderTable::new();
    let mut init_done = false;
    let init_deadline = Instant::now() + Duration::from_secs(5);
    let mut got_positions = false;
    while Instant::now() < init_deadline && !(init_done && got_positions) {
        if let Some(msg) = c.try_recv()? {
            if msg.msg_type() == tags::MSG_EXECUTION_REPORT
                && msg.get_str(tags::MASS_STATUS_REQ_ID).is_some()
            {
                if msg.get_str(tags::TOT_NUM_REPORTS) != Some("0")
                    && let Some(o) = er_to_local_order(&msg)
                {
                    orders.insert(o.order_id.clone(), o);
                }
                if msg.get_str(tags::LAST_RPT_REQUESTED) == Some("Y") {
                    init_done = true;
                }
            } else if msg.msg_type() == tags::MSG_POSITION_REPORT {
                let _ = tx.send(UiMsg::Balances(parse_positions(&msg)));
                got_positions = true;
            }
        }
    }
    send_order_table(&orders, tx);
    let _ = tx.send(UiMsg::Log(format!(
        "[OE] synced: {} active orders",
        orders.len()
    )));
    // Init complete — clear the backoff (see run_md_session_once).
    *attempt = 0;

    // --- Main loop: send orders, process unsolicited ERs ---

    loop {
        c.maintain_heartbeat()?;

        // Process order commands from the UI.
        while let Ok(cmd) = order_rx.try_recv() {
            match cmd {
                OrderCmd::NewOrder {
                    symbol,
                    side,
                    price,
                    qty,
                    clord_id,
                } => {
                    let nos = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
                        .str_tag(tags::CL_ORD_ID, &clord_id)
                        .str_tag(tags::SYMBOL, &symbol)
                        .str_tag(tags::SIDE, &side)
                        .str_tag(tags::ORD_TYPE, "2") // Limit
                        .str_tag(tags::PRICE, &price)
                        .str_tag(tags::ORDER_QTY, &qty)
                        .str_tag(tags::TIME_IN_FORCE, "1") // GTC
                        .str_tag(tags::ACCOUNT, "1");
                    // Send errors here are fatal for this session — bubble
                    // up so the reconnect loop reopens the connection
                    // rather than silently dropping subsequent orders.
                    c.send_builder(nos)?;
                    let side_str = if side == "1" { "BUY" } else { "SELL" };
                    let _ = tx.send(UiMsg::Log(format!(
                        "[OE] {clord_id}: {side_str} {qty}@{price} {symbol}"
                    )));
                }
            }
        }

        // Read responses.
        if let Some(msg) = c.try_recv()? {
            let mt = msg.msg_type();
            if mt == tags::MSG_EXECUTION_REPORT {
                handle_exec_report(&msg, &mut orders, tx);
            } else if mt == tags::MSG_POSITION_REPORT {
                let _ = tx.send(UiMsg::Balances(parse_positions(&msg)));
            } else if mt == tags::MSG_ORDER_CANCEL_REJECT {
                let text = msg.get_str(tags::TEXT).unwrap_or("unknown");
                let clord = msg.get_str(tags::CL_ORD_ID).unwrap_or("?");
                let _ = tx.send(UiMsg::OrderAck(format!(
                    "{clord}: CANCEL REJECTED ({text})"
                )));
            }
        }
    }
}

/// Build a LocalOrder from an ER (mass-status or unsolicited Placed).
fn er_to_local_order(msg: &FixMessage<'_>) -> Option<LocalOrder> {
    let order_id = msg.get_str(tags::ORDER_ID)?;
    Some(LocalOrder {
        order_id: order_id.to_owned(),
        clord_id: msg.get_str(tags::CL_ORD_ID).unwrap_or("-").to_owned(),
        symbol: msg.get_str(tags::SYMBOL).unwrap_or("-").to_owned(),
        side: match msg.get_str(tags::SIDE) {
            Some("1") => "BUY",
            Some("2") => "SELL",
            _ => "?",
        },
        qty: msg.get_str(tags::ORDER_QTY).unwrap_or("-").to_owned(),
        price: msg.get_str(tags::PRICE).unwrap_or("-").to_owned(),
        leaves_qty: msg.get_str(tags::LEAVES_QTY).unwrap_or("-").to_owned(),
    })
}

/// Process an unsolicited execution report, update the local order table.
fn handle_exec_report(msg: &FixMessage<'_>, orders: &mut OrderTable, tx: &Sender<UiMsg>) {
    let exec_type = msg.get_str(tags::EXEC_TYPE).unwrap_or("?");
    let clord = msg.get_str(tags::CL_ORD_ID).unwrap_or("?");
    let order_id = msg.get_str(tags::ORDER_ID).unwrap_or("?");

    match exec_type {
        "0" => {
            // Placed — add to table.
            if let Some(o) = er_to_local_order(msg) {
                orders.insert(o.order_id.clone(), o);
            }
            let _ = tx.send(UiMsg::OrderAck(format!("{clord}: PLACED")));
            send_order_table(orders, tx);
        }
        "F" => {
            // Fill — update leaves_qty, remove if fully filled.
            let px = msg.get_str(tags::LAST_PX).unwrap_or("?");
            let qty = msg.get_str(tags::LAST_SHARES).unwrap_or("?");
            let leaves = msg.get_str(tags::LEAVES_QTY).unwrap_or("0");
            if let Some(o) = orders.get_mut(order_id) {
                o.leaves_qty = leaves.to_owned();
                if leaves == "0" {
                    orders.remove(order_id);
                }
            }
            let _ = tx.send(UiMsg::OrderAck(format!("{clord}: FILL {qty}@{px}")));
            send_order_table(orders, tx);
        }
        "4" => {
            // Cancelled — remove from table.
            orders.remove(order_id);
            let _ = tx.send(UiMsg::OrderAck(format!("{clord}: CANCELLED")));
            send_order_table(orders, tx);
        }
        "8" => {
            // Rejected — not on the book, just notify.
            let text = msg.get_str(tags::TEXT).unwrap_or("");
            let _ = tx.send(UiMsg::OrderAck(format!("{clord}: REJECTED ({text})")));
        }
        "I" => {
            // Order status (from mass status) — ignore in main loop,
            // these only matter during initial sync.
        }
        _ => {
            let _ = tx.send(UiMsg::Log(format!(
                "[OE] {clord}: ExecType={exec_type} (unhandled)"
            )));
        }
    }
}

/// Extract balance entries from a PositionReport (35=AP).
fn parse_positions(msg: &FixMessage<'_>) -> Vec<String> {
    let fields: Vec<&Field<'_>> = msg.fields_iter().collect();
    let (mut out, mut i) = (Vec::new(), 0);
    while i < fields.len() {
        if fields[i].tag == tags::CURRENCY {
            let s = |off, tag: u32| {
                fields
                    .get(i + off)
                    .filter(|f: &&&Field<'_>| f.tag == tag)
                    .and_then(|f| std::str::from_utf8(f.value).ok())
                    .unwrap_or("0")
            };
            let ccy = std::str::from_utf8(fields[i].value).unwrap_or("?");
            out.push(format!(
                "{ccy}: free={}  reserved={}",
                s(1, tags::LONG_QTY),
                s(2, tags::SHORT_QTY)
            ));
            i += 3;
        } else {
            i += 1;
        }
    }
    out
}

// --- Bot session ---------------------------------------------------------------

/// Synthetic order-flow bot on its own FIX session.
///
/// Opens a second FIX connection to the OE gateway (logging in with a
/// distinct SenderCompID — configured with a different Ed25519 key to
/// keep its per-key Melin request-seq namespace disjoint from the human
/// trader's) and continuously submits `NewOrderSingle` messages whose
/// rate traces a sine wave across time.
///
/// Each order picks a random account from a pool that does NOT include
/// the human trader's account (so balances, active-orders, and positions
/// in the TUI only reflect the user's own activity). Execution reports
/// from the bot session are drained and discarded — the bot keeps no
/// local order state.
fn run_bot_session(addr: &str, sender: &str, target: &str, tx: &Sender<UiMsg>) {
    let mut attempt: u32 = 0;
    loop {
        match run_bot_session_once(addr, sender, target, tx, &mut attempt) {
            Ok(()) => return, // graceful — UI gone
            Err(e) => {
                let delay = backoff_delay(attempt);
                let _ = tx.send(UiMsg::Log(format!(
                    "[BOT] disconnected ({e}); reconnecting in {} ms (attempt {})",
                    delay.as_millis(),
                    attempt + 1,
                )));
                thread::sleep(delay);
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

/// One bot session lifetime: connect, then submit orders forever at a
/// constant rate, with per-order prices that track a sinusoidal mid.
/// State (RNG, ClOrdID counter) resets on reconnect — the bot has no
/// engine-side state to preserve, and the gateway hands out fresh
/// OrderIds regardless of ClOrdID reuse across sessions. `attempt`
/// is reset to 0 once Logon completes (see `run_md_session_once`).
fn run_bot_session_once(
    addr: &str,
    sender: &str,
    target: &str,
    tx: &Sender<UiMsg>,
    attempt: &mut u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tx.send(UiMsg::Log(format!(
        "[BOT] connecting to {addr} as {sender}…"
    )));

    let mut c = FixClient::connect(addr, sender, target, 30)?;
    *attempt = 0;
    // Short poll timeout: the bot only drains ERs opportunistically
    // between bursts, so it should not block reading when no data is in.
    c.set_read_timeout(Some(Duration::from_millis(1)))?;
    let _ = tx.send(UiMsg::Log("[BOT] connected".into()));

    // mid(t) = MID_BASE + MID_AMP · sin(2π · t / PERIOD). Submission
    // rate is flat at BOT_RATE — the visible sinusoid lives in the
    // *price* now, so the book cluster walks up and down over each
    // PERIOD_SECS cycle. See `bot::bot_mid_price` for the curve.
    let mut rng_state: u64 = 0xC0FF_EE00_DEAD_BEEF;
    let sleep_dur = Duration::from_secs_f64(1.0 / bot::BOT_RATE);

    let start = Instant::now();
    // u64 ClOrdID suffix. The gateway assigns fresh Melin OrderIds from
    // its per-session id_map, so bot ClOrdIDs only need to be unique
    // within this FIX session.
    let mut next_clord_id: u64 = 1;
    let mut last_report = start;
    let mut sent_since_report: u64 = 0;
    // Diagnostic counters. The bot's expected steady state is ~zero
    // rejections; sustained non-zero values usually mean the gateway
    // assigned an OrderId the engine has already seen (e.g. after a
    // server restart with a persisted journal — the engine's per-account
    // OrderId HWM survives but the gateway's id_map starts fresh).
    let mut rejections_total: u64 = 0;
    let mut rejections_logged: u32 = 0;
    const MAX_REJECTION_LOGS: u32 = 5;

    loop {
        let t = start.elapsed().as_secs_f64();
        let order = bot::next_bot_order(&mut rng_state, t);
        let clord = format!("BOT{next_clord_id}");
        next_clord_id += 1;
        let nos = bot::build_bot_nos(&clord, &order);

        c.send_builder(nos)?;
        sent_since_report += 1;

        if last_report.elapsed() >= Duration::from_secs(2) {
            // Drain queued ERs before reporting so the TCP buffer doesn't
            // grow unbounded over long runs. Tally rejections (35=8 with
            // 150=8) and log the first few with reason text so the cause
            // is observable instead of silent. A read error here is a
            // real disconnect — bubble it up to trigger reconnect.
            while let Some(msg) = c.try_recv()? {
                if msg.get_str(tags::EXEC_TYPE) != Some("8") {
                    continue;
                }
                rejections_total += 1;
                if rejections_logged < MAX_REJECTION_LOGS {
                    let reason = msg.get_str(tags::TEXT).unwrap_or("?");
                    let clord_id = msg.get_str(tags::CL_ORD_ID).unwrap_or("?");
                    let _ = tx.send(UiMsg::Log(format!(
                        "[BOT] rejected clord={clord_id} reason={reason}"
                    )));
                    rejections_logged += 1;
                }
            }

            let actual = sent_since_report as f64 / last_report.elapsed().as_secs_f64();
            let mid = bot::bot_mid_price(t);
            let _ = tx.send(UiMsg::Log(format!(
                "[BOT] t={t:5.1}s mid={mid:>7.2} sent={actual:>5.1}/s rej={rejections_total}"
            )));
            last_report = Instant::now();
            sent_since_report = 0;
        }

        thread::sleep(sleep_dur);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- backoff_delay --

    #[test]
    fn backoff_delay_floors_at_50ms() {
        // Floor matters: a zero/negative computation would burn CPU in the
        // reconnect loop. Sample multiple times so jitter can't hide it.
        for attempt in 0..20 {
            for _ in 0..50 {
                assert!(backoff_delay(attempt) >= Duration::from_millis(50));
            }
        }
    }

    #[test]
    fn backoff_delay_caps_at_about_5s() {
        // Cap = 5000 ms + up to +99 ms jitter. Anything above that means
        // the saturating shift overflowed or the cap regressed.
        for attempt in 6..32 {
            for _ in 0..50 {
                assert!(backoff_delay(attempt) <= Duration::from_millis(5_100));
            }
        }
    }

    #[test]
    fn backoff_delay_grows_with_attempt_pre_cap() {
        // Coarse monotonicity: average of many samples should grow with
        // attempt up to the cap. Jitter prevents a per-call assertion.
        let avg = |attempt: u32| {
            let n = 200u64;
            let total: u64 = (0..n)
                .map(|_| backoff_delay(attempt).as_millis() as u64)
                .sum();
            total / n
        };
        // Compare neighbouring attempts where doubling clearly dominates
        // ±100 ms jitter. attempt=2 → ~400 ms, attempt=4 → ~1600 ms.
        assert!(avg(4) > avg(2));
    }

    /// Build a FIX message from a builder, parse it back.
    fn build_and_parse(builder: FixMessageBuilder) -> Vec<u8> {
        builder.build("SENDER", "TARGET", 1)
    }

    fn make_exec_report(exec_type: &str, order_id: &str, clord: &str) -> FixMessageBuilder {
        FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
            .str_tag(tags::ORDER_ID, order_id)
            .str_tag(tags::CL_ORD_ID, clord)
            .str_tag(tags::EXEC_ID, "1")
            .str_tag(tags::EXEC_TYPE, exec_type)
            .str_tag(tags::ORD_STATUS, "0")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORDER_QTY, "100")
            .str_tag(tags::PRICE, "50000")
            .str_tag(tags::LEAVES_QTY, "100")
            .str_tag(tags::CUM_QTY, "0")
            .str_tag(tags::AVG_PX, "0")
    }

    // -- er_to_local_order tests --

    #[test]
    fn er_to_local_order_extracts_fields() {
        let raw = build_and_parse(make_exec_report("0", "42", "abc"));
        let msg = FixMessage::parse(&raw).unwrap();
        let o = er_to_local_order(&msg).unwrap();
        assert_eq!(o.order_id, "42");
        assert_eq!(o.clord_id, "abc");
        assert_eq!(o.symbol, "BTC/USD");
        assert_eq!(o.side, "BUY");
        assert_eq!(o.qty, "100");
        assert_eq!(o.price, "50000");
        assert_eq!(o.leaves_qty, "100");
    }

    #[test]
    fn er_to_local_order_missing_order_id_returns_none() {
        let raw = build_and_parse(
            FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
                .str_tag(tags::CL_ORD_ID, "abc")
                .str_tag(tags::EXEC_TYPE, "0"),
        );
        let msg = FixMessage::parse(&raw).unwrap();
        assert!(er_to_local_order(&msg).is_none());
    }

    // -- handle_exec_report tests --

    #[test]
    fn handle_placed_inserts_order() {
        let (tx, rx) = mpsc::channel();
        let mut orders = OrderTable::new();
        let raw = build_and_parse(make_exec_report("0", "42", "abc"));
        let msg = FixMessage::parse(&raw).unwrap();

        handle_exec_report(&msg, &mut orders, &tx);

        assert!(orders.contains_key("42"));
        assert_eq!(orders["42"].clord_id, "abc");
        // Should have sent OrderAck and ActiveOrders messages.
        let msgs: Vec<_> = rx.try_iter().collect();
        assert!(msgs.len() >= 2);
    }

    #[test]
    fn handle_fill_updates_leaves() {
        let (tx, _rx) = mpsc::channel();
        let mut orders = OrderTable::new();

        // Place first.
        let raw = build_and_parse(make_exec_report("0", "42", "abc"));
        let msg = FixMessage::parse(&raw).unwrap();
        handle_exec_report(&msg, &mut orders, &tx);

        // Partial fill — build without the base LEAVES_QTY so the
        // parser sees only the fill's value.
        let raw = build_and_parse(
            FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
                .str_tag(tags::ORDER_ID, "42")
                .str_tag(tags::CL_ORD_ID, "abc")
                .str_tag(tags::EXEC_ID, "2")
                .str_tag(tags::EXEC_TYPE, "F")
                .str_tag(tags::ORD_STATUS, "1")
                .str_tag(tags::SYMBOL, "BTC/USD")
                .str_tag(tags::SIDE, "1")
                .str_tag(tags::ORDER_QTY, "100")
                .str_tag(tags::PRICE, "50000")
                .str_tag(tags::LAST_PX, "50000")
                .str_tag(tags::LAST_SHARES, "30")
                .str_tag(tags::LEAVES_QTY, "70")
                .str_tag(tags::CUM_QTY, "30")
                .str_tag(tags::AVG_PX, "50000"),
        );
        let msg = FixMessage::parse(&raw).unwrap();
        handle_exec_report(&msg, &mut orders, &tx);

        assert_eq!(orders["42"].leaves_qty, "70");
    }

    #[test]
    fn handle_fill_removes_when_fully_filled() {
        let (tx, _rx) = mpsc::channel();
        let mut orders = OrderTable::new();

        let raw = build_and_parse(make_exec_report("0", "42", "abc"));
        let msg = FixMessage::parse(&raw).unwrap();
        handle_exec_report(&msg, &mut orders, &tx);

        let raw = build_and_parse(
            FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
                .str_tag(tags::ORDER_ID, "42")
                .str_tag(tags::CL_ORD_ID, "abc")
                .str_tag(tags::EXEC_ID, "3")
                .str_tag(tags::EXEC_TYPE, "F")
                .str_tag(tags::ORD_STATUS, "2")
                .str_tag(tags::SYMBOL, "BTC/USD")
                .str_tag(tags::SIDE, "1")
                .str_tag(tags::ORDER_QTY, "100")
                .str_tag(tags::PRICE, "50000")
                .str_tag(tags::LAST_PX, "50000")
                .str_tag(tags::LAST_SHARES, "100")
                .str_tag(tags::LEAVES_QTY, "0")
                .str_tag(tags::CUM_QTY, "100")
                .str_tag(tags::AVG_PX, "50000"),
        );
        let msg = FixMessage::parse(&raw).unwrap();
        handle_exec_report(&msg, &mut orders, &tx);

        assert!(!orders.contains_key("42"));
    }

    #[test]
    fn handle_cancelled_removes_order() {
        let (tx, _rx) = mpsc::channel();
        let mut orders = OrderTable::new();

        let raw = build_and_parse(make_exec_report("0", "42", "abc"));
        let msg = FixMessage::parse(&raw).unwrap();
        handle_exec_report(&msg, &mut orders, &tx);

        let raw = build_and_parse(make_exec_report("4", "42", "abc"));
        let msg = FixMessage::parse(&raw).unwrap();
        handle_exec_report(&msg, &mut orders, &tx);

        assert!(orders.is_empty());
    }

    #[test]
    fn handle_rejected_does_not_insert() {
        let (tx, _rx) = mpsc::channel();
        let mut orders = OrderTable::new();

        let raw = build_and_parse(
            make_exec_report("8", "42", "abc").str_tag(tags::TEXT, "insufficient funds"),
        );
        let msg = FixMessage::parse(&raw).unwrap();
        handle_exec_report(&msg, &mut orders, &tx);

        assert!(orders.is_empty());
    }

    #[test]
    fn handle_fill_for_unknown_order_is_graceful() {
        let (tx, _rx) = mpsc::channel();
        let mut orders = OrderTable::new();

        // Fill for order not in table — should not panic.
        let raw = build_and_parse(
            FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
                .str_tag(tags::ORDER_ID, "999")
                .str_tag(tags::CL_ORD_ID, "abc")
                .str_tag(tags::EXEC_ID, "1")
                .str_tag(tags::EXEC_TYPE, "F")
                .str_tag(tags::ORD_STATUS, "2")
                .str_tag(tags::SYMBOL, "BTC/USD")
                .str_tag(tags::SIDE, "1")
                .str_tag(tags::ORDER_QTY, "10")
                .str_tag(tags::PRICE, "50000")
                .str_tag(tags::LAST_PX, "50000")
                .str_tag(tags::LAST_SHARES, "10")
                .str_tag(tags::LEAVES_QTY, "0")
                .str_tag(tags::CUM_QTY, "10")
                .str_tag(tags::AVG_PX, "50000"),
        );
        let msg = FixMessage::parse(&raw).unwrap();
        handle_exec_report(&msg, &mut orders, &tx);

        assert!(orders.is_empty());
    }

    // -- parse_snapshot tests --

    #[test]
    fn parse_snapshot_extracts_bids_and_asks() {
        let raw = build_and_parse(
            FixMessageBuilder::new(tags::MSG_MD_SNAPSHOT)
                .str_tag(tags::SYMBOL, "BTC/USD")
                // Bid entry.
                .str_tag(tags::MD_ENTRY_TYPE, "0")
                .str_tag(tags::MD_ENTRY_PX, "49000")
                .str_tag(tags::MD_ENTRY_SIZE, "5")
                .str_tag(tags::NUMBER_OF_ORDERS, "2")
                // Ask entry.
                .str_tag(tags::MD_ENTRY_TYPE, "1")
                .str_tag(tags::MD_ENTRY_PX, "51000")
                .str_tag(tags::MD_ENTRY_SIZE, "3")
                .str_tag(tags::NUMBER_OF_ORDERS, "1"),
        );
        let msg = FixMessage::parse(&raw).unwrap();
        let (bids, asks) = parse_snapshot(&msg);

        assert_eq!(bids.len(), 1);
        assert_eq!(bids[0].0, "49000");
        assert_eq!(bids[0].1, "5");
        assert_eq!(bids[0].2, "2");

        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0].0, "51000");
        assert_eq!(asks[0].1, "3");
    }

    #[test]
    fn parse_snapshot_empty_book() {
        let raw = build_and_parse(
            FixMessageBuilder::new(tags::MSG_MD_SNAPSHOT).str_tag(tags::SYMBOL, "BTC/USD"),
        );
        let msg = FixMessage::parse(&raw).unwrap();
        let (bids, asks) = parse_snapshot(&msg);
        assert!(bids.is_empty());
        assert!(asks.is_empty());
    }
}
