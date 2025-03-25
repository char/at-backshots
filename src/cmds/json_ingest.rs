use anyhow::Result;
use backshots::{
    get_app_config, ingest::likes_test::ingest_json, storage::live_guards::LiveWriteHandle,
    AppContext,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().compact())
        .with(
            "json_ingest=debug,backshots=debug,backshots::ingest=info"
                .parse::<EnvFilter>()
                .unwrap(),
        )
        .init();

    let cfg = get_app_config()?;
    let mut app = AppContext::new(&cfg)?;
    let mut storage = LiveWriteHandle::latest(&app)?;
    ingest_json(&mut app, &mut storage)?;

    Ok(())
}
