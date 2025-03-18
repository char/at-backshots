use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use backshots::{http::listen, ingest::firehose::ingest_firehose, AppState};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
pub async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer().compact())
        .with("backshots=debug".parse::<EnvFilter>().unwrap())
        .init();

    let addr: SocketAddr = "127.0.0.1:3000".parse()?;
    let app = Arc::new(AppState::new("http://127.0.0.1:2485".into())?);

    {
        let app = Arc::clone(&app);
        tokio::task::spawn(async move {
            // match ingest_firehose(&app, "127.0.0.1", 2482, false).await {
            match ingest_firehose(&app, "bsky.network", 443, true).await {
                Ok(_) => {}
                Err(e) => tracing::error!("ingest error: {:?}", e),
            }
        });
    }

    println!("Listening at: http://{addr}/ ...");
    listen(app, addr).await?;

    Ok(())
}
