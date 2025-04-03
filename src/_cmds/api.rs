use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    net::SocketAddr,
    sync::Arc,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use anyhow::Result;
use hyper::{body::Incoming, header, server::conn::http1, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use backshots::{
    data::{
        did::resolve_did,
        record::{resolve_collection, resolve_rkey, RecordId},
    },
    get_app_config,
    http::{body_full, Body},
    storage::{compacted::CompactedStorageReader, live_guards::LiveReadHandle},
    AppConfig, AppContext,
};

fn get_response(cfg: Arc<AppConfig>, req: Request<Incoming>) -> Result<Response<Body>> {
    let mut app = AppContext::new(&cfg)?;
    let path = req.uri().path();

    match (req.method(), path) {
        (&Method::GET, "/") => Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(body_full("backshots running..."))?),

        (&Method::GET, "/status") => {
            let db = app.db;

            let collection_count: u64 =
                db.query_row("SELECT COUNT(id) FROM collections", (), |row| row.get(0))?;
            let backlink_count: u64 = db.query_row(
                "SELECT count FROM counts WHERE key = 'backlinks'",
                (),
                |row| row.get(0),
            )?;
            let rkey_count: u64 =
                db.query_row("SELECT COUNT(id) FROM outline_rkeys", (), |row| row.get(0))?;
            let did_count: u64 =
                db.query_row("SELECT COUNT(id) FROM outline_dids", (), |row| row.get(0))?;

            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(body_full(format!(
                    r#"status:
collections: {}
backlinks: {}
outline rkeys: {}
non-zplc dids: {}"#,
                    collection_count, backlink_count, rkey_count, did_count,
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

            let Ok(record_id) = RecordId::from_at_uri(&mut app, at_uri) else {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body_full("'uri' param was not a valid at-uri"))?);
            };

            let mut sources = BTreeSet::<RecordId>::new();
            {
                // TODO: we should be able to persist the open stores per-thread
                // and then only refresh them periodically instead of opening them on every request

                let mut statement = app
                    .db
                    .prepare("SELECT id, name, type FROM data_stores ORDER BY id ASC")?;
                let stores = statement.query_map((), |row| {
                    let id: u64 = row.get(0)?;
                    let name: String = row.get(1)?;
                    let type_ = row.get_ref(2)?;
                    let is_live = type_.as_str()?.eq("live");
                    Ok((id, name, is_live))
                })?;
                for store in stores {
                    let (_store_id, name, is_live) = store?;
                    if is_live {
                        let mut storage = LiveReadHandle::new(&app, name)?;
                        storage.read_backlinks(&record_id, &mut sources)?;
                    } else {
                        let mut storage =
                            CompactedStorageReader::new(app.data_dir.join("compacted").join(name))?;
                        storage.read_backlinks(&record_id, &mut sources)?;
                    }
                }
            }

            let mut backlink_uris = BTreeMap::<String, BTreeSet<String>>::new();
            for source in sources {
                let did = resolve_did(&app, source.did)?;
                let collection = resolve_collection(&app, source.collection)?;
                let rkey = resolve_rkey(&app, source.rkey)?;
                let links = backlink_uris.entry(collection.clone()).or_default();
                links.insert(format!("at://{did}/{collection}/{rkey}"));
            }

            // manually stringify the json just because we know the exact shape,
            // + putting the data into tinyjson means converting BTreeMaps
            // into HashMaps and BTreeSets into Vecs :/
            let json = {
                let mut json = String::new();
                json.push('{');

                let mut first = true;
                for (collection, links) in backlink_uris {
                    if !first {
                        json.push(',');
                    }
                    first = false;

                    json.push_str(&tinyjson::JsonValue::String(collection).stringify()?);
                    json.push(':');

                    json.push('[');
                    {
                        let mut first = true;
                        for link in links {
                            if !first {
                                json.push(',');
                            }
                            first = false;
                            json.push_str(&tinyjson::JsonValue::String(link).stringify()?);
                        }
                    }
                    json.push(']');
                }
                json.push('}');
                json
            };

            Ok(Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(body_full(json))?)
        }

        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(body_full("Not Found"))?),
    }
}

async fn serve(cfg: Arc<AppConfig>, req: Request<Incoming>) -> Result<Response<Body>> {
    match tokio::task::spawn_blocking(move || get_response(cfg, req)).await? {
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

pub async fn listen(cfg: Arc<AppConfig>, addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;

    loop {
        let (stream, _client_addr) = listener.accept().await?;
        let io = TokioIo::new(stream);

        let cfg = Arc::clone(&cfg);

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(
                    io,
                    hyper::service::service_fn(move |req| serve(Arc::clone(&cfg), req)),
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

    let addr: SocketAddr = std::env::var("BIND_ADDRESS")
        .ok()
        .as_deref()
        .unwrap_or("127.0.0.1:3000")
        .parse()?;
    let cfg = Arc::new(get_app_config()?);
    println!("Listening at: http://{addr}/ ...");
    listen(cfg, addr).await?;

    Ok(())
}
