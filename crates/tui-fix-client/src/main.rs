//! TUI trading client that speaks FIX 4.4 to both the oe-gateway
//! (order entry) and md-gateway (market data).
//!
//! Usage:
//!   melin-tui-fix-client --oe-addr 127.0.0.1:9000 --md-addr 127.0.0.1:9001 \
//!     --sender CLIENT --oe-target MELIN-OE --md-target MELIN-MD

pub mod fix_client;

use std::io;

use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Paragraph};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse args.
    let args: Vec<String> = std::env::args().collect();
    let mut oe_addr = "127.0.0.1:9000".to_string();
    let mut md_addr = "127.0.0.1:9001".to_string();
    let mut _sender = "CLIENT".to_string();
    let mut oe_target = "MELIN-OE".to_string();
    let mut md_target = "MELIN-MD".to_string();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--oe-addr" => {
                i += 1;
                oe_addr = args.get(i).cloned().unwrap_or_default();
            }
            "--md-addr" => {
                i += 1;
                md_addr = args.get(i).cloned().unwrap_or_default();
            }
            "--sender" => {
                i += 1;
                _sender = args.get(i).cloned().unwrap_or_default();
            }
            "--oe-target" => {
                i += 1;
                oe_target = args.get(i).cloned().unwrap_or_default();
            }
            "--md-target" => {
                i += 1;
                md_target = args.get(i).cloned().unwrap_or_default();
            }
            _ => {
                eprintln!(
                    "usage: melin-tui-fix-client [--oe-addr ADDR] [--md-addr ADDR] \
                     [--sender ID] [--oe-target ID] [--md-target ID]"
                );
                std::process::exit(1);
            }
        }
        i += 1;
    }

    // Setup terminal.
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let status = format!(
        "OE: {} → {}  |  MD: {} → {}  |  Press 'q' to quit",
        oe_addr, oe_target, md_addr, md_target
    );

    // Main render loop.
    loop {
        terminal.draw(|f| {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Min(5),    // Order book
                    Constraint::Min(5),    // Trades / active orders
                    Constraint::Length(3), // Status bar
                ])
                .split(f.area());

            let top_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(chunks[0]);

            // Order book placeholder.
            let book = Paragraph::new(vec![Line::from("  (connect to md-gateway for live book)")])
                .block(Block::default().title(" Order Book ").borders(Borders::ALL));
            f.render_widget(book, top_chunks[0]);

            // Balances placeholder.
            let balances =
                Paragraph::new(vec![Line::from("  (connect to oe-gateway for balances)")])
                    .block(Block::default().title(" Balances ").borders(Borders::ALL));
            f.render_widget(balances, top_chunks[1]);

            let bottom_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(chunks[1]);

            // Active orders placeholder.
            let orders = Paragraph::new(vec![Line::from(
                "  (connect to oe-gateway for active orders)",
            )])
            .block(
                Block::default()
                    .title(" Active Orders ")
                    .borders(Borders::ALL),
            );
            f.render_widget(orders, bottom_chunks[0]);

            // Trades placeholder.
            let trades = Paragraph::new(vec![Line::from("  (connect to md-gateway for trades)")])
                .block(
                    Block::default()
                        .title(" Recent Trades ")
                        .borders(Borders::ALL),
                );
            f.render_widget(trades, bottom_chunks[1]);

            // Status bar.
            let status_bar = Paragraph::new(Line::from(status.as_str()))
                .style(Style::default().fg(Color::Cyan))
                .block(Block::default().borders(Borders::TOP));
            f.render_widget(status_bar, chunks[2]);
        })?;

        // Poll for input (100ms timeout for responsive UI).
        if event::poll(std::time::Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
            && key.code == KeyCode::Char('q')
        {
            break;
        }
    }

    // Restore terminal.
    disable_raw_mode()?;
    crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}
