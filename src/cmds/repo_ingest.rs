use std::sync::Arc;

use backshots::{ingest::repo_car::ingest_repo_archive, storage::BacklinkStorage, AppState};
use tokio::{fs::File, io::AsyncReadExt, time::Instant};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main(flavor = "current_thread")]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().compact())
        .with(
            "repo_ingest=debug,backshots=debug,backshots::ingest=info"
                .parse::<EnvFilter>()
                .unwrap(),
        )
        .init();

    let app = Arc::new(AppState::new(
        "/dev/shm/backshots/data",
        "http://127.0.0.1:2485".into(),
    )?);

    let mut storage = BacklinkStorage::new("/dev/shm/backshots/data")?;
    let repo = {
        let mut v = vec![];
        let mut f = File::open("./target/repo-pet.bun.how-2025-03-17T19_47_03.277Z.car").await?;
        f.read_to_end(&mut v).await?;
        v
    };

    let now = Instant::now();
    tracing::debug!("starting ingest");
    ingest_repo_archive(
        &app,
        &mut storage,
        "did:plc:fp5zf7du5zntbwwcxkk3dppd".into(),
        &mut std::io::Cursor::new(&repo),
    )
    .await?;
    let elapsed = now.elapsed();
    tracing::info!(
        car = "repo-pet.bun.how-2025-03-17T19_47_03.277Z.car",
        ?elapsed,
        "finished indexing repo archive"
    );

    Ok(())
}
