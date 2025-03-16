use anyhow::Result;
use backshots::{http::listen, ingest::ingest, AppState};
use std::{net::SocketAddr, sync::Arc};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
pub async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with("backshots=debug".parse::<EnvFilter>().unwrap())
        .init();

    let addr: SocketAddr = "127.0.0.1:3000".parse()?;

    let db = sled::Config::default()
        .path("./data")
        .cache_capacity(1024 * 1024 * 1024)
        .mode(sled::Mode::LowSpace)
        .open()?;
    let app = Arc::new(AppState::new("https://zplc.cerulea.blue".into(), db)?);

    {
        let app = Arc::clone(&app);
        tokio::task::spawn(async move {
            // match ingest(&app, "127.0.0.1", 2482, false).await {
            match ingest(&app, "bsky.network", 443, true).await {
                Ok(_) => {}
                Err(e) => tracing::error!("ingest error: {:?}", e),
            }
        });
    }

    println!("Listening at: http://{addr}/ ...");
    listen(app, addr).await?;

    Ok(())
}
