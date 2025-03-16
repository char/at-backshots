use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::{
    body::{Bytes, Incoming},
    header,
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

async fn serve(app: Arc<AppState>, req: Request<Incoming>) -> Result<Response<Body>> {
    let path = req.uri().path();

    match (req.method(), path) {
        (&Method::GET, "/") => Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(body_full("backshots running..."))?),

        (&Method::GET, "/xrpc/_status") => Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(body_full(format!(
                r#"status:
collections: {}
records: {}
rkeys: {}
dids: {}"#,
                app.db_collections.len(),
                app.db_records.len(),
                app.db_rkeys.len(),
                app.db_dids.len(),
            )))?),

        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(header::CONTENT_TYPE, "text/plain")
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
