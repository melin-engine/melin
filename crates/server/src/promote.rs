//! Promotion trigger endpoint — plain TCP listener that signals a replica
//! to promote itself to primary.
//!
//! An operator connects and sends `PROMOTE\n`. The listener sets an
//! `AtomicBool` flag that the replica's receive loop checks, then
//! responds with `OK\n` and closes.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use tracing::{debug, error, info};

/// Spawn the promotion listener on a dedicated thread.
///
/// Returns the join handle. The listener accepts one connection at a time,
/// checks for the "PROMOTE" command, and sets the flag. The thread exits
/// when `shutdown` is set or after a successful promotion.
pub fn spawn(
    bind_addr: SocketAddr,
    promote: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("promote-listener".into())
        .spawn(move || run(bind_addr, &promote, &shutdown))
        .expect("failed to spawn promote listener thread")
}

fn run(bind_addr: SocketAddr, promote: &AtomicBool, shutdown: &AtomicBool) {
    let listener = match TcpListener::bind(bind_addr) {
        Ok(l) => l,
        Err(e) => {
            error!(addr = %bind_addr, error = %e, "promote listener bind failed");
            return;
        }
    };
    // Non-blocking accept so we can check the shutdown flag periodically.
    listener
        .set_nonblocking(true)
        .expect("set promote listener nonblocking");

    info!(addr = %bind_addr, "promote listener started");

    loop {
        if shutdown.load(Ordering::Relaxed) || promote.load(Ordering::Relaxed) {
            return;
        }

        match listener.accept() {
            Ok((stream, peer)) => {
                debug!(peer = %peer, "promote connection accepted");
                if handle_connection(stream, promote) {
                    info!("promotion triggered");
                    return;
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                debug!(error = %e, "promote listener accept error");
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// Handle a single connection. Returns `true` if promotion was triggered.
fn handle_connection(mut stream: TcpStream, promote: &AtomicBool) -> bool {
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();

    let cloned = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            debug!(error = %e, "failed to clone promote stream");
            return false;
        }
    };
    let mut reader = BufReader::new(cloned);

    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        debug!("failed to read from promote connection");
        return false;
    }

    if line.trim() == "PROMOTE" {
        promote.store(true, Ordering::Release);
        let _ = stream.write_all(b"OK\n");
        let _ = stream.flush();
        true
    } else {
        debug!(received = %line.trim(), "unexpected promote command");
        let _ = stream.write_all(b"ERR unknown command\n");
        let _ = stream.flush();
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};

    /// Helper: bind to an ephemeral port and return the listener + address.
    fn ephemeral_listener() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        (listener, addr)
    }

    #[test]
    fn promote_command_sets_flag() {
        let (listener, addr) = ephemeral_listener();
        drop(listener); // free the port for the promote listener

        let promote = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let handle = spawn(addr, Arc::clone(&promote), Arc::clone(&shutdown));

        // Give listener time to start.
        std::thread::sleep(Duration::from_millis(200));

        // Connect and send PROMOTE.
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(b"PROMOTE\n").unwrap();
        stream.flush().unwrap();

        let mut reader = BufReader::new(stream);
        let mut response = String::new();
        reader.read_line(&mut response).unwrap();
        assert_eq!(response.trim(), "OK");

        // Flag should be set.
        assert!(promote.load(Ordering::Acquire));

        handle.join().unwrap();
    }

    #[test]
    fn invalid_command_rejected() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let promote = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _handle = spawn(addr, Arc::clone(&promote), Arc::clone(&shutdown));

        std::thread::sleep(Duration::from_millis(200));

        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(b"INVALID\n").unwrap();
        stream.flush().unwrap();

        let mut reader = BufReader::new(stream);
        let mut response = String::new();
        reader.read_line(&mut response).unwrap();
        assert!(response.starts_with("ERR"));

        // Flag should NOT be set.
        assert!(!promote.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }
}
