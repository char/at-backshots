use std::sync::Arc;

use backshots::{
    ingest::likes_test::ingest_json, storage::live_writer::LiveStorageWriter, AppState,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main(flavor = "current_thread")]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().compact())
        .with(
            "json_ingest=debug,backshots=debug,backshots::ingest=info"
                .parse::<EnvFilter>()
                .unwrap(),
        )
        .init();

    let app = Arc::new(AppState::new(
        "/dev/shm/backshots/data",
        "http://127.0.0.1:2485".into(),
    )?);

    let storage = LiveStorageWriter::new("/dev/shm/backshots/data")?;
    ingest_json(&app, storage).await?;

    Ok(())
}
