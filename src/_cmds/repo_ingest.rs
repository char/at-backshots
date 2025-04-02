use std::{fs::File, io::Read, time::Instant};

use anyhow::Result;
use backshots::{
    get_app_config, ingest::repo_car::ingest_repo_archive, storage::live::LiveStorageWriter,
    AppContext,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().compact())
        .with(
            "repo_ingest=debug,backshots=debug,backshots::ingest=info"
                .parse::<EnvFilter>()
                .unwrap(),
        )
        .init();

    let cfg = get_app_config()?;
    let mut app = AppContext::new(&cfg)?;

    let mut storage = LiveStorageWriter::new("/dev/shm/backshots/data")?;
    let repo = {
        let mut v = vec![];
        let mut f = File::open("./target/repo-pet.bun.how-2025-03-17T19_47_03.277Z.car")?;
        f.read_to_end(&mut v)?;
        v
    };

    let now = Instant::now();
    tracing::debug!("starting ingest");
    ingest_repo_archive(
        &mut app,
        &mut storage,
        "did:plc:fp5zf7du5zntbwwcxkk3dppd".into(),
        &mut std::io::Cursor::new(&repo),
    )?;
    let elapsed = now.elapsed();
    tracing::info!(
        car = "repo-pet.bun.how-2025-03-17T19_47_03.277Z.car",
        ?elapsed,
        "finished indexing repo archive"
    );

    Ok(())
}
