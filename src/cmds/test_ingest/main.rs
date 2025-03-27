use anyhow::Result;
use backshots::{get_app_config, storage::live_guards::LiveWriteHandle, AppContext};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod firehose_db;
mod json;

fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().compact())
        .with(
            "test_ingest=debug,backshots=debug,backshots::ingest=info"
                .parse::<EnvFilter>()
                .unwrap(),
        )
        .init();

    let cfg = get_app_config()?;
    let mut app = AppContext::new(&cfg)?;
    let mut storage = LiveWriteHandle::latest(&app)?;
    // json::ingest_json(&mut app, &mut storage)?;

    tracing::info!("starting test ingest…");
    firehose_db::ingest_firehose_db(&mut app, &mut storage)?;
    tracing::info!("test ingest done!");

    Ok(())
}
