use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use anyhow::Result;
use hyper::{body::Incoming, header, server::conn::http1, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use backshots::{
    data::record::RecordId,
    http::{body_full, Body},
    storage::BacklinkStorage,
    AppState,
};

async fn get_response(app: Arc<AppState>, req: Request<Incoming>) -> Result<Response<Body>> {
    let path = req.uri().path();

    match (req.method(), path) {
        (&Method::GET, "/") => Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(body_full("backshots running..."))?),

        (&Method::GET, "/status") => {
            let db = app.db();
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(body_full(format!(
                    r#"status:
collections: {}
backlinks: {} (targets: {})
outline rkeys: {}
non-zplc dids: {}"#,
                    db.query_row("SELECT COUNT(id) FROM collections", (), |row| row
                        .get::<_, u64>(0))?,
                    db.query_row(
                        "SELECT count FROM counts WHERE key = 'backlinks'",
                        (),
                        |row| row.get::<_, u64>(0),
                    )?,
                    0, // TODO: read targets count from backlink storage
                    db.query_row("SELECT COUNT(id) FROM outline_rkeys", (), |row| row
                        .get::<_, u64>(0))?,
                    db.query_row("SELECT COUNT(id) FROM outline_dids", (), |row| row
                        .get::<_, u64>(0))?,
                )))?)
        }

        (&Method::GET, "/links") => {
            let q: HashMap<_, _> = req
                .uri()
                .query()
                .map(|v| form_urlencoded::parse(v.as_bytes()).collect())
                .unwrap_or_default();

            let Some(at_uri) = q.get("uri") else {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body_full("'uri' param missing"))?);
            };

            let Ok(record_id) = RecordId::from_at_uri(&app, at_uri).await else {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body_full("'uri' param was not a valid at-uri"))?);
            };

            let mut storage = BacklinkStorage::new("/dev/shm/backshots/data")?;

            let mut backlinks = HashSet::<String>::new();
            for link in storage.read_backlinks(&record_id)? {
                let did = app.resolve_did(link.source.did()).await?;
                let collection = app.resolve_collection(link.source.collection as u32)?;
                let rkey = app.resolve_rkey(link.source.rkey)?;
                backlinks.insert(format!("at://{did}/{collection}/{rkey}"));
            }

            Ok(Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(body_full(
                    tinyjson::JsonValue::Array(
                        backlinks
                            .into_iter()
                            .map(tinyjson::JsonValue::String)
                            .collect::<Vec<_>>(),
                    )
                    .stringify()?,
                ))?)
        }

        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(body_full("Not Found"))?),
    }
}

async fn serve(app: Arc<AppState>, req: Request<Incoming>) -> Result<Response<Body>> {
    match get_response(app, req).await {
        Ok(res) => Ok(res),
        Err(err) => {
            tracing::error!("error handling request: {err:?}");
            Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(body_full("Internal Server Error"))?)
        }
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
