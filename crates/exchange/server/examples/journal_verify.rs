//! Read a journal file and print its final sequence number and BLAKE3 chain hash.
//!
//! Usage: cargo run --release -p melin-server --example journal_verify -- <path>

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: journal_verify <journal-path>");
    let mut reader =
        melin_journal::JournalReader::<melin_trading::trading_event::TradingEvent>::open(
            path.as_ref(),
        )
        .expect("open journal");

    let mut count = 0u64;
    let mut last_seq = 0u64;
    loop {
        match reader.next_entry() {
            Ok(Some(entry)) => {
                count += 1;
                last_seq = entry.sequence;
            }
            Ok(None) => break,
            Err(e) => {
                eprintln!("error at entry {}: {e}", count + 1);
                break;
            }
        }
    }

    println!("entries:    {count}");
    println!("last_seq:   {last_seq}");
    match reader.chain_hash() {
        Some(h) => {
            print!("chain_hash: ");
            for b in h {
                print!("{b:02x}");
            }
            println!();
        }
        None => println!("chain_hash: (none — v5 journal)"),
    }
}
