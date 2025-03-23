use std::{net::SocketAddr, sync::Arc};

use backshots::{web::listen, AppState};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().compact())
        .with("api=debug,backshots=debug".parse::<EnvFilter>().unwrap())
        .init();

    let addr: SocketAddr = "127.0.0.1:3000".parse()?;
    let app = Arc::new(AppState::new(
        "/dev/shm/backshots/data",
        "http://127.0.0.1:2485".into(),
    )?);
    println!("Listening at: http://{addr}/ ...");
    listen(app, addr).await?;

    Ok(())
}
