mod engine;
mod server;
mod session;

use trading_protocol::tcp::TcpTransportListener;

use server::ServerConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ServerConfig::default();
    let listener = TcpTransportListener::bind(config.bind_addr).await?;
    server::run(listener, config).await
}
