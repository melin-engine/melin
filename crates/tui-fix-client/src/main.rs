//! TUI trading client that speaks FIX 4.4 to both the oe-gateway and md-gateway.
//!
//! Usage:
//!   melin-tui-fix-client --oe-addr 127.0.0.1:9000 --md-addr 127.0.0.1:9001 \
//!     --sender CLIENT --oe-target MELIN-OE --md-target MELIN-MD

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
    status: String,
    md_ok: bool,
    oe_ok: bool,
    form: OrderForm,
    /// Rolling log of recent events (newest at the end, capped at 50).
    logs: Vec<String>,
    /// Optional file for persistent logging.
    log_file: Option<std::fs::File>,
}

impl App {
    fn new(status: String) -> Self {
        Self {
            bids: vec![],
            asks: vec![],
            active_orders: vec![],
            balances: vec![],
            status,
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
    fn drain(&mut self, rx: &Receiver<UiMsg>) {
        while let Ok(m) = rx.try_recv() {
            match m {
                UiMsg::MdStatus(ok, s) => {
                    self.md_ok = ok;
                    if !s.is_empty() {
                        self.status = s;
                    }
                }
                UiMsg::Book(b, a) => {
                    self.bids = b;
                    self.asks = a;
                }
                UiMsg::OeStatus(ok, s) => {
                    self.oe_ok = ok;
                    if !s.is_empty() {
                        self.status = s;
                    }
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
            _ => {
                eprintln!(
                    "usage: melin-tui-fix-client [--oe-addr ADDR] [--md-addr ADDR] [--sender ID] [--oe-target ID] [--md-target ID]"
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
    let oe_tx = tx;
    let (oe_a, oe_t, oe_s) = (oe_addr.clone(), oe_target.clone(), sender.clone());
    thread::spawn(move || run_oe_session(&oe_a, &oe_s, &oe_t, &oe_tx, &order_rx));

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
        Paragraph::new(Line::from(app.status.as_str()))
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

// --- MD session ---------------------------------------------------------------

fn run_md_session(addr: &str, sender: &str, target: &str, tx: &Sender<UiMsg>) {
    let send_err = |e: &dyn std::fmt::Display| {
        let _ = tx.send(UiMsg::MdStatus(false, format!("MD: {e}")));
    };
    let _ = tx.send(UiMsg::Log(format!("[MD] connecting to {addr}…")));
    let mut c = match FixClient::connect(addr, sender, target, 30) {
        Ok(c) => c,
        Err(e) => {
            send_err(&e);
            return;
        }
    };
    let _ = tx.send(UiMsg::MdStatus(true, String::new()));
    let _ = tx.send(UiMsg::Log(
        "[MD] connected, sending SecurityListRequest".into(),
    ));

    // Discover symbols via SecurityListRequest.
    let slr = FixMessageBuilder::new(tags::MSG_SECURITY_LIST_REQUEST)
        .str_tag(tags::SECURITY_REQ_ID, "SLR1")
        .str_tag(tags::SECURITY_LIST_REQUEST_TYPE, "0");
    if let Err(e) = c.send_builder(slr) {
        send_err(&e);
        return;
    }

    let symbols: Vec<String> = match c.recv() {
        Ok(msg) if msg.msg_type() == tags::MSG_SECURITY_LIST => {
            let syms: Vec<String> = msg
                .fields_iter()
                .filter(|f| f.tag == tags::SYMBOL)
                .filter_map(|f| std::str::from_utf8(f.value).ok())
                .map(String::from)
                .collect();
            let _ = tx.send(UiMsg::Log(format!("[MD] symbols: {}", syms.join(", "))));
            syms
        }
        Ok(msg) => {
            let mt = std::str::from_utf8(msg.msg_type()).unwrap_or("?");
            let _ = tx.send(UiMsg::Log(format!(
                "[MD] unexpected response to SLR: 35={mt}"
            )));
            vec![]
        }
        Err(e) => {
            send_err(&e);
            return;
        }
    };

    if let Err(e) = c.set_read_timeout(Some(Duration::from_millis(100))) {
        send_err(&e);
        return;
    }

    // Periodically re-request snapshots (every 1s) since incremental
    // updates (X) aren't wired yet. This ensures the book display
    // reflects orders placed after the initial snapshot.
    let mut last_request = Instant::now() - Duration::from_secs(10);
    let mut req_n: u64 = 0;
    loop {
        if last_request.elapsed() >= Duration::from_secs(1) {
            for sym in &symbols {
                req_n += 1;
                let mdr = FixMessageBuilder::new(tags::MSG_MARKET_DATA_REQUEST)
                    .str_tag(tags::MD_REQ_ID, &format!("MD{req_n}"))
                    .str_tag(tags::SUBSCRIPTION_REQUEST_TYPE, "0") // Snapshot
                    .str_tag(tags::MARKET_DEPTH, "0")
                    .u64_tag(tags::NO_RELATED_SYM, 1)
                    .str_tag(tags::SYMBOL, sym);
                if let Err(e) = c.send_builder(mdr) {
                    send_err(&e);
                    return;
                }
            }
            last_request = Instant::now();
        }
        match c.try_recv() {
            Ok(Some(msg)) => {
                let mt_str = std::str::from_utf8(msg.msg_type()).unwrap_or("?");
                let _ = tx.send(UiMsg::Log(format!(
                    "[MD<] 35={mt_str} 262={} entries={}",
                    msg.get_str(tags::MD_REQ_ID).unwrap_or("-"),
                    msg.get_str(tags::NO_MD_ENTRIES).unwrap_or("-"),
                )));
                if msg.msg_type() == tags::MSG_MD_SNAPSHOT {
                    let (b, a) = parse_snapshot(&msg);
                    if tx.send(UiMsg::Book(b, a)).is_err() {
                        return;
                    }
                }
            }
            Ok(_) => {}
            Err(e) => {
                send_err(&e);
                return;
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

fn run_oe_session(
    addr: &str,
    sender: &str,
    target: &str,
    tx: &Sender<UiMsg>,
    order_rx: &Receiver<OrderCmd>,
) {
    let send_err = |e: &dyn std::fmt::Display| {
        let _ = tx.send(UiMsg::OeStatus(false, format!("OE: {e}")));
    };
    let _ = tx.send(UiMsg::Log(format!("[OE] connecting to {addr}…")));
    let mut c = match FixClient::connect(addr, sender, target, 30) {
        Ok(c) => c,
        Err(e) => {
            send_err(&e);
            return;
        }
    };
    let _ = tx.send(UiMsg::OeStatus(true, String::new()));
    let _ = tx.send(UiMsg::Log("[OE] connected".into()));
    if let Err(e) = c.set_read_timeout(Some(Duration::from_millis(100))) {
        send_err(&e);
        return;
    }

    let (mut last_q, mut msr_n, mut pr_n) = (Instant::now() - Duration::from_secs(10), 0u64, 0u64);
    let mut pending_orders: Vec<String> = Vec::new();
    loop {
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
                    let _ = tx.send(UiMsg::Log(format!(
                        "[OE>] D 11={clord_id} 55={symbol} 54={side} 44={price} 38={qty} 40=2 59=1"
                    )));
                    match c.send_builder(nos) {
                        Ok(()) => {
                            let side_str = if side == "1" { "BUY" } else { "SELL" };
                            let _ = tx.send(UiMsg::OrderAck(format!(
                                "Sent {clord_id}: {side_str} {qty}@{price} {symbol}"
                            )));
                        }
                        Err(e) => {
                            let _ = tx.send(UiMsg::OrderAck(format!("Send failed: {e}")));
                        }
                    }
                }
            }
        }

        // Periodic queries.
        if last_q.elapsed() >= Duration::from_secs(2) {
            msr_n += 1;
            let _ = tx.send(UiMsg::Log(format!("[OE>] AF 584=MSR{msr_n}")));
            let msr = FixMessageBuilder::new(tags::MSG_ORDER_MASS_STATUS_REQUEST)
                .str_tag(tags::MASS_STATUS_REQ_ID, &format!("MSR{msr_n}"))
                .str_tag(tags::MASS_STATUS_REQ_TYPE, "1");
            if let Err(e) = c.send_builder(msr) {
                send_err(&e);
                return;
            }
            pr_n += 1;
            let _ = tx.send(UiMsg::Log(format!("[OE>] AN 710=PR{pr_n}")));
            let pr = FixMessageBuilder::new(tags::MSG_REQUEST_FOR_POSITIONS)
                .str_tag(tags::POS_REQ_ID, &format!("PR{pr_n}"))
                .str_tag(tags::POS_REQ_TYPE, "0")
                .str_tag(tags::ACCOUNT, "1");
            if let Err(e) = c.send_builder(pr) {
                send_err(&e);
                return;
            }
            pending_orders.clear();
            last_q = Instant::now();
        }

        // Read responses.
        match c.try_recv() {
            Ok(Some(msg)) => {
                // Log every received FIX message.
                let mt = msg.msg_type();
                let mt_str = std::str::from_utf8(mt).unwrap_or("?");
                let _ = tx.send(UiMsg::Log(format!(
                    "[OE<] 35={mt_str} 150={} 39={} 37={} 11={} 584={} 58={}",
                    msg.get_str(tags::EXEC_TYPE).unwrap_or("-"),
                    msg.get_str(tags::ORD_STATUS).unwrap_or("-"),
                    msg.get_str(tags::ORDER_ID).unwrap_or("-"),
                    msg.get_str(tags::CL_ORD_ID).unwrap_or("-"),
                    msg.get_str(tags::MASS_STATUS_REQ_ID).unwrap_or("-"),
                    msg.get_str(tags::TEXT).unwrap_or("-"),
                )));
                if mt == tags::MSG_EXECUTION_REPORT {
                    if msg.get_str(tags::MASS_STATUS_REQ_ID).is_some() {
                        // Mass status response — accumulate.
                        if msg.get_str(tags::TOT_NUM_REPORTS) != Some("0") {
                            pending_orders.extend(parse_mass_status(&msg));
                        }
                        if msg.get_str(tags::LAST_RPT_REQUESTED) == Some("Y") {
                            let _ = tx.send(UiMsg::ActiveOrders(pending_orders.clone()));
                            pending_orders.clear();
                        }
                    } else {
                        // Regular execution report (order ack/reject/fill).
                        let exec_type = msg.get_str(tags::EXEC_TYPE).unwrap_or("?");
                        let ord_status = msg.get_str(tags::ORD_STATUS).unwrap_or("?");
                        let clord = msg.get_str(tags::CL_ORD_ID).unwrap_or("?");
                        let text = msg.get_str(tags::TEXT).unwrap_or("");
                        let ack = match exec_type {
                            "0" => format!("{clord}: PLACED (status={ord_status})"),
                            "F" => {
                                let px = msg.get_str(tags::LAST_PX).unwrap_or("?");
                                let qty = msg.get_str(tags::LAST_SHARES).unwrap_or("?");
                                format!("{clord}: FILL {qty}@{px}")
                            }
                            "4" => format!("{clord}: CANCELLED"),
                            "8" => format!("{clord}: REJECTED ({text})"),
                            _ => {
                                format!("{clord}: ExecType={exec_type} status={ord_status} {text}")
                            }
                        };
                        let _ = tx.send(UiMsg::OrderAck(ack));
                    }
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
            Ok(None) => {}
            Err(e) => {
                send_err(&e);
                return;
            }
        }
    }
}

fn parse_mass_status(msg: &FixMessage<'_>) -> Vec<String> {
    if msg.get_str(tags::TOT_NUM_REPORTS) == Some("0") {
        return vec![];
    }
    let g = |t| msg.get_str(t).unwrap_or("-");
    let side = match msg.get_str(tags::SIDE) {
        Some("1") => "BUY",
        Some("2") => "SELL",
        _ => "?",
    };
    vec![format!(
        "{} {} {} {}@{} leaves={} st={}",
        g(tags::ORDER_ID),
        g(tags::SYMBOL),
        side,
        g(tags::ORDER_QTY),
        g(tags::PRICE),
        g(tags::LEAVES_QTY),
        g(tags::ORD_STATUS)
    )]
}

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
