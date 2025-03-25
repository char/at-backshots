use anyhow::Result;
use backshots::{
    ingest::likes_test::ingest_json, storage::live_writer::LiveStorageWriter, AppConfig, AppContext,
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

    let cfg = AppConfig {
        zplc_base: "http://127.0.0.1:2485".into(),
        data_dir: "/dev/shm/backshots/data".into(),
    };
    let mut app = AppContext::new(&cfg)?;

    let storage = LiveStorageWriter::new("/dev/shm/backshots/data")?;
    ingest_json(&mut app, storage)?;

    Ok(())
}
