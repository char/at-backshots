use std::sync::Arc;

use backshots::{ingest::likes_test::ingest_json, storage::BacklinkStorage, AppState};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().compact())
        .with("backshots=debug".parse::<EnvFilter>().unwrap())
        .init();

    let app = Arc::new(AppState::new("http://127.0.0.1:2485".into())?);

    let storage = BacklinkStorage::new("./data/")?;
    ingest_json(&app, storage).await?;

    Ok(())
}
