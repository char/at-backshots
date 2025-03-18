use anyhow::{Context, Result};
use hyper::{body::Incoming, client::conn::http1, Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

use crate::http::Body;

pub async fn fetch(req: Request<Body>) -> Result<Response<Incoming>> {
    if !matches!(
        req.version(),
        hyper::Version::HTTP_10 | hyper::Version::HTTP_11
    ) {
        anyhow::bail!("fetch(..) only supports HTTP/1 and HTTP/1.1");
    }

    match req.uri().scheme_str() {
        Some("http") => {
            let authority = req.uri().authority().context("no authority")?;
            let host = authority.host();
            let port = authority.port_u16().unwrap_or(80);
            let stream = TcpStream::connect((host, port)).await?;
            let (mut sender, conn) = http1::handshake(TokioIo::new(stream)).await?;
            tokio::task::spawn(async move {
                if let Err(err) = conn.await {
                    tracing::warn!("connection failed: {err:?}");
                }
            });
            let res = sender.send_request(req).await?;
            Ok(res)
        }
        Some("https") => todo!("tls not supported yet"),
        _ => anyhow::bail!("unsupported url scheme"),
    }
}
