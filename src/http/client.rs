use anyhow::{Context, Result};
use hyper::{body::Incoming, client::conn::http1, Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;

use crate::http::{tls::open_tls_stream, Body};

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
        Some("https") => {
            let authority = req.uri().authority().context("no authority")?;
            let host = authority.host();
            let port = authority.port_u16().unwrap_or(443);
            let plain_stream = TcpStream::connect((host, port)).await?;
            let tls_stream =
                open_tls_stream(plain_stream, ServerName::try_from(host.to_owned())?).await?;
            let (mut sender, conn) = http1::handshake(TokioIo::new(tls_stream)).await?;
            tokio::task::spawn(async move {
                if let Err(err) = conn.await {
                    tracing::warn!("connection failed: {err:?}");
                }
            });
            let res = sender.send_request(req).await?;
            Ok(res)
        }
        _ => anyhow::bail!("unsupported url scheme"),
    }
}
