use backshots::{
    backfill::db::open_backfill_db, firehose::ingest_firehose, get_app_config, AppContext,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().compact())
        .with(
            "firehose_ingest=debug,backshots=debug"
                .parse::<EnvFilter>()
                .unwrap(),
        )
        .init();

    let cfg = get_app_config()?;
    let mut app = AppContext::new(&cfg)?;
    let backfill_db = open_backfill_db(&cfg)?;
    let ingest = async move {
        match ingest_firehose(
            &mut app,
            None, /* Some(&backfill_db) */
            "bsky.network",
            443,
            true,
        )
        .await
        {
            Ok(_) => {}
            Err(e) => tracing::error!("ingest error: {:?}", e),
        }
    };

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = ingest => {}
    };

    tracing::info!("shutting down!");

    Ok(())
}
