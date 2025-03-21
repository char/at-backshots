use std::sync::Arc;

use backshots::{ingest::firehose::ingest_firehose, storage::BacklinkStorage, AppState};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().compact())
        .with("backshots=debug".parse::<EnvFilter>().unwrap())
        .init();

    let app = Arc::new(AppState::new(
        "/dev/shm/backshots/data",
        "http://127.0.0.1:2485".into(),
    )?);

    let storage = BacklinkStorage::new("/dev/shm/backshots/data")?;
    match ingest_firehose(&app, storage, "bsky.network", 443, true).await {
        Ok(_) => {}
        Err(e) => tracing::error!("ingest error: {:?}", e),
    }

    Ok(())
}
