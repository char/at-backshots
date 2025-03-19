use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use backshots::{
    ingest::{firehose::ingest_firehose, likes_test::ingest_json},
    storage::BacklinkStorage,
    web::listen,
    AppState,
};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
pub async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer().compact())
        .with("backshots=debug".parse::<EnvFilter>().unwrap())
        .init();

    let addr: SocketAddr = "127.0.0.1:3000".parse()?;
    let app = Arc::new(AppState::new("http://127.0.0.1:2485".into())?);
    let storage = BacklinkStorage::new("./data/", Arc::clone(&app.targets_count))?;

    {
        let app = Arc::clone(&app);
        tokio::task::spawn(async move {
            // match ingest_json(&app, storage).await {
            match ingest_firehose(&app, storage, "bsky.network", 443, true).await {
                Ok(_) => {}
                Err(e) => tracing::error!("ingest error: {:?}", e),
            }
        });
    }

    println!("Listening at: http://{addr}/ ...");
    listen(app, addr).await?;

    Ok(())
}
