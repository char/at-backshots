use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpStream;
use tokio_rustls::{
    client::TlsStream,
    rustls::{self, pki_types::ServerName},
    TlsConnector,
};

pub async fn open_tls_stream(
    tcp_stream: TcpStream,
    domain_tls: ServerName<'static>,
) -> Result<TlsStream<TcpStream>> {
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let client_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let tls_connector = TlsConnector::from(Arc::new(client_config));

    let stream = tls_connector.connect(domain_tls, tcp_stream).await?;
    Ok(stream)
}
