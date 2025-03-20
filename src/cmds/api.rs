use std::{net::SocketAddr, sync::Arc};

use backshots::{web::listen, AppState};

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let addr: SocketAddr = "127.0.0.1:3000".parse()?;
    let app = Arc::new(AppState::new("http://127.0.0.1:2485".into())?);
    println!("Listening at: http://{addr}/ ...");
    listen(app, addr).await?;

    Ok(())
}
