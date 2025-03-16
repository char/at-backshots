use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::{
    body::{Bytes, Incoming},
    server::conn::http1,
    Method, Request, Response, StatusCode,
};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use crate::AppState;

pub type Body = BoxBody<Bytes, hyper::Error>;
pub fn body_empty() -> Body {
    Empty::<Bytes>::new().map_err(|e| match e {}).boxed()
}
pub fn body_full<T: Into<Bytes>>(chunk: T) -> Body {
    Full::new(chunk.into()).map_err(|e| match e {}).boxed()
}

async fn serve(_app: Arc<AppState>, req: Request<Incoming>) -> Result<Response<Body>> {
    let path = req.uri().path();

    match (req.method(), path) {
        (&Method::GET, "/") => Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain")
            .body(body_full("backshots running..."))?),

        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header("Content-Type", "text/plain")
            .body(body_full("Not Found"))?),
    }
}

pub async fn listen(app: Arc<AppState>, addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;

    loop {
        let (stream, _client_addr) = listener.accept().await?;
        let io = TokioIo::new(stream);

        let app = Arc::clone(&app);
        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(
                    io,
                    hyper::service::service_fn(move |req| serve(Arc::clone(&app), req)),
                )
                .with_upgrades()
                .await
            {
                eprintln!("Error handling connection: {err:?}")
            }
        });
    }
}
