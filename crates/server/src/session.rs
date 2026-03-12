//! Per-connection session management.
//!
//! Each TCP connection spawns two tokio tasks:
//! - **Reader**: decodes requests from the wire and forwards them to the engine.
//! - **Writer**: encodes responses from the engine and sends them to the client.
//!
//! This split allows reading and writing to proceed concurrently without
//! blocking each other.

use std::net::SocketAddr;

use tokio::sync::mpsc;

use trading_protocol::codec;
use trading_protocol::message::{ConnectionId, EngineCommand, Response};
use trading_protocol::transport::{TransportRead, TransportWrite};

/// Maximum encoded response size. Responses are small (execution reports),
/// so 128 bytes is generous.
const MAX_RESPONSE_BUF: usize = 128;

/// Spawn reader and writer tasks for a new connection.
///
/// The reader task decodes requests and sends `EngineCommand::Request`
/// messages to the engine. On disconnect, it sends `EngineCommand::Disconnected`.
///
/// The writer task receives `Response` messages from the engine and encodes
/// them to the wire. It flushes after each `BatchEnd`.
pub fn spawn_session<R: TransportRead, W: TransportWrite>(
    connection_id: ConnectionId,
    reader: R,
    writer: W,
    engine_tx: mpsc::Sender<EngineCommand>,
    response_rx: mpsc::Receiver<Response>,
    addr: SocketAddr,
) {
    tokio::spawn(reader_task(connection_id, reader, engine_tx, addr));
    tokio::spawn(writer_task(connection_id, writer, response_rx, addr));
}

/// Read frames from the transport, decode requests, and forward to the engine.
async fn reader_task<R: TransportRead>(
    connection_id: ConnectionId,
    mut reader: R,
    engine_tx: mpsc::Sender<EngineCommand>,
    addr: SocketAddr,
) {
    loop {
        let frame = match reader.read_frame().await {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                // Clean disconnect.
                eprintln!("[session] client {addr} disconnected");
                break;
            }
            Err(e) => {
                eprintln!("[session] read error from {addr}: {e}");
                break;
            }
        };

        let request = match codec::decode_request(&frame) {
            Ok(req) => req,
            Err(e) => {
                eprintln!("[session] decode error from {addr}: {e}");
                // Skip malformed messages rather than disconnecting — the
                // client may have a codec bug but other messages may be valid.
                continue;
            }
        };

        let cmd = EngineCommand::Request {
            connection_id,
            request,
        };
        if engine_tx.send(cmd).await.is_err() {
            // Engine shut down.
            eprintln!("[session] engine channel closed, dropping {addr}");
            break;
        }
    }

    // Notify the engine that this connection is gone so it can clean up
    // the response sender from its connection table.
    let _ = engine_tx
        .send(EngineCommand::Disconnected { connection_id })
        .await;
}

/// Receive responses from the engine and encode them to the transport.
async fn writer_task<W: TransportWrite>(
    connection_id: ConnectionId,
    mut writer: W,
    mut response_rx: mpsc::Receiver<Response>,
    addr: SocketAddr,
) {
    let mut buf = [0u8; MAX_RESPONSE_BUF];

    while let Some(response) = response_rx.recv().await {
        let is_batch_end = matches!(response, Response::BatchEnd);

        let written = match codec::encode_response(&response, &mut buf) {
            Ok(n) => n,
            Err(e) => {
                eprintln!(
                    "[session] encode error for connection {}: {e}",
                    connection_id.0
                );
                continue;
            }
        };

        // write_frame expects the payload (tag + fields), not the length prefix.
        // Our encode_response writes [length(4) | tag+payload], so skip the prefix.
        if let Err(e) = writer.write_frame(&buf[4..written]).await {
            eprintln!("[session] write error to {addr}: {e}");
            break;
        }

        // Flush after each batch to minimize latency — the client is waiting
        // for all reports from its request before proceeding.
        if is_batch_end && let Err(e) = writer.flush().await {
            eprintln!("[session] flush error to {addr}: {e}");
            break;
        }
    }
}
