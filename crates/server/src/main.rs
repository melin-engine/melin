use trading_protocol::tcp::TcpTransportListener;
use trading_server::server::ServerConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .with_thread_names(true)
        .init();

    let config = ServerConfig::default();
    let listener = TcpTransportListener::bind(config.bind_addr).await?;
    trading_server::server::run(listener, config).await
}
