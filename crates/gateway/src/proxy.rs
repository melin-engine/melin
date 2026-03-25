//! Transparent TCP proxy between clients and the engine server.
//!
//! For each accepted client, opens a dedicated connection to the engine
//! and forwards frames bidirectionally. Two OS threads per client:
//! one for client→engine, one for engine→client. This matches the
//! blocking I/O model used throughout the codebase.
//!
//! This is intentionally simple — no protocol interpretation, no
//! multiplexing, no connection pooling. The gateway just moves bytes.
//! Protocol-aware features (market data, subscriptions) will be added
//! later.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;

/// Configuration for the gateway proxy.
pub struct GatewayConfig {
    /// Address the gateway listens on for client connections.
    pub listen_addr: SocketAddr,
    /// Address of the engine server to forward requests to.
    pub engine_addr: SocketAddr,
}

/// Run the gateway proxy. Blocks forever, accepting client connections.
///
/// For each client, spawns two threads to forward traffic in both
/// directions. When either direction hits an error or disconnect,
/// both halves shut down.
pub fn run(config: &GatewayConfig) -> io::Result<()> {
    let listener = TcpListener::bind(config.listen_addr)?;
    tracing::info!(
        listen = %config.listen_addr,
        engine = %config.engine_addr,
        "gateway listening",
    );

    loop {
        let (client_stream, client_addr) = listener.accept()?;
        let engine_addr = config.engine_addr;

        thread::spawn(move || {
            if let Err(e) = handle_client(client_stream, client_addr, engine_addr) {
                tracing::debug!(client = %client_addr, error = %e, "client session ended");
            }
        });
    }
}

/// Handle a single client connection by proxying to the engine.
fn handle_client(
    client_stream: TcpStream,
    client_addr: SocketAddr,
    engine_addr: SocketAddr,
) -> io::Result<()> {
    client_stream.set_nodelay(true)?;

    let engine_stream = TcpStream::connect(engine_addr)?;
    engine_stream.set_nodelay(true)?;

    tracing::debug!(client = %client_addr, "connected to engine");

    // Keep shutdown handles so either direction can tear down the other.
    let client_shutdown = client_stream.try_clone()?;
    let engine_shutdown = engine_stream.try_clone()?;

    // Clone streams for the reverse direction threads.
    let client_read = client_stream.try_clone()?;
    let client_write = client_stream;
    let engine_read = engine_stream.try_clone()?;
    let engine_write = engine_stream;

    // client → engine
    let c2e = thread::spawn({
        move || {
            let result = forward(client_read, engine_write);
            tracing::debug!(client = %client_addr, "client→engine closed");
            // Client disconnected — shut down engine side to unblock
            // the engine→client forward.
            let _ = engine_shutdown.shutdown(std::net::Shutdown::Both);
            result
        }
    });

    // engine → client (runs on this thread)
    let result = forward(engine_read, client_write);
    tracing::debug!(client = %client_addr, "engine→client closed");
    // Engine disconnected — shut down client side to unblock
    // the client→engine forward.
    let _ = client_shutdown.shutdown(std::net::Shutdown::Both);

    let _ = c2e.join();

    result
}

/// Copy bytes from `src` to `dst` until EOF or error.
///
/// Uses a 4 KiB stack buffer — small enough to stay in L1 cache, large
/// enough to carry many protocol frames per syscall. No heap allocation.
fn forward(mut src: impl Read, mut dst: impl Write) -> io::Result<()> {
    // 4 KiB: fits in L1, carries ~50-100 protocol frames per read.
    let mut buf = [0u8; 4096];
    loop {
        let n = match src.read(&mut buf) {
            Ok(0) => return Ok(()), // clean EOF
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::ConnectionReset => return Ok(()),
            Err(e) => return Err(e),
        };
        dst.write_all(&buf[..n])?;
        dst.flush()?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_protocol::blocking::{BlockingFrameReader, BlockingFrameWriter};
    use melin_protocol::codec;
    use melin_protocol::message::{Request, ResponseKind};
    use melin_protocol::types::*;

    /// Start a mock engine that echoes back a BatchEnd for each request.
    fn mock_engine(listener: TcpListener) {
        // Accept one connection, serve it until disconnect.
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());
        let mut writer = BlockingFrameWriter::new(stream);
        let mut buf = [0u8; 128];

        while let Ok(Some(_frame)) = reader.read_frame() {
            let written = codec::encode_response(&ResponseKind::BatchEnd, &mut buf).unwrap();
            writer.write_frame(&buf[4..written]).unwrap();
            writer.flush().unwrap();
        }
    }

    #[test]
    fn proxy_forwards_request_and_response() {
        // Start mock engine.
        let engine_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let engine_addr = engine_listener.local_addr().unwrap();
        thread::spawn(move || mock_engine(engine_listener));

        // Start gateway.
        let gw_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let gw_addr = gw_listener.local_addr().unwrap();
        thread::spawn(move || {
            let (client_stream, client_addr) = gw_listener.accept().unwrap();
            handle_client(client_stream, client_addr, engine_addr).ok();
        });

        // Connect client to gateway.
        let stream = TcpStream::connect(gw_addr).unwrap();
        stream.set_nodelay(true).unwrap();
        let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());
        let mut writer = BlockingFrameWriter::new(stream);

        // Send a request through the gateway.
        let mut encode_buf = [0u8; 128];
        let request = Request::CancelOrder {
            symbol: Symbol(1),
            account: AccountId(1),
            order_id: OrderId(42),
        };
        let written = codec::encode_request(&request, 0, &mut encode_buf).unwrap();
        writer.write_frame(&encode_buf[4..written]).unwrap();
        writer.flush().unwrap();

        // Should receive BatchEnd back through the gateway.
        let frame = reader.read_frame().unwrap().unwrap();
        let response = codec::decode_response(frame).unwrap();
        assert_eq!(response, ResponseKind::BatchEnd);
    }

    #[test]
    fn proxy_handles_multiple_requests() {
        let engine_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let engine_addr = engine_listener.local_addr().unwrap();
        thread::spawn(move || mock_engine(engine_listener));

        let gw_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let gw_addr = gw_listener.local_addr().unwrap();
        thread::spawn(move || {
            let (client_stream, client_addr) = gw_listener.accept().unwrap();
            handle_client(client_stream, client_addr, engine_addr).ok();
        });

        let stream = TcpStream::connect(gw_addr).unwrap();
        stream.set_nodelay(true).unwrap();
        let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());
        let mut writer = BlockingFrameWriter::new(stream);

        let mut encode_buf = [0u8; 128];

        for i in 0..10 {
            let request = Request::CancelOrder {
                symbol: Symbol(1),
                account: AccountId(1),
                order_id: OrderId(i),
            };
            let written = codec::encode_request(&request, 0, &mut encode_buf).unwrap();
            writer.write_frame(&encode_buf[4..written]).unwrap();
            writer.flush().unwrap();

            let frame = reader.read_frame().unwrap().unwrap();
            let response = codec::decode_response(frame).unwrap();
            assert_eq!(response, ResponseKind::BatchEnd);
        }
    }

    #[test]
    fn proxy_closes_on_client_disconnect() {
        let engine_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let engine_addr = engine_listener.local_addr().unwrap();
        thread::spawn(move || mock_engine(engine_listener));

        let gw_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let gw_addr = gw_listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (client_stream, client_addr) = gw_listener.accept().unwrap();
            handle_client(client_stream, client_addr, engine_addr)
        });

        // Connect and immediately drop.
        let stream = TcpStream::connect(gw_addr).unwrap();
        drop(stream);

        // Gateway should exit cleanly.
        let result = handle.join().unwrap();
        assert!(result.is_ok());
    }
}
