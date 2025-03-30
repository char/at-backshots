use std::time::Duration;

use anyhow::Result;
use backshots::{
    data::did::resolve_did,
    get_app_config,
    http::{body_empty, client::fetch},
    ingest::repo_car::ingest_repo_archive,
    storage::{live::LiveStorageWriter, live_guards::LiveWriteHandle},
    AppConfig, AppContext,
};
use http_body_util::BodyExt;
use hyper::{header, Request};
use rusqlite::{fallible_iterator::FallibleIterator, Batch, Connection};
use tinyjson::JsonValue;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

fn open_backfill_db(cfg: &AppConfig) -> Result<Connection> {
    let backfill_db = Connection::open(cfg.data_dir.join("backfill.db"))?;
    let mut batch = Batch::new(&backfill_db, include_str!("./db.sql"));
    while let Some(mut stmt) = batch.next()? {
        stmt.execute(())?;
    }
    Ok(backfill_db)
}

async fn get_did_document(did: &str) -> Result<JsonValue> {
    let did_doc: JsonValue = if did.starts_with("did:plc:") {
        let res = fetch(
            Request::builder()
                .uri(format!("http://127.0.0.1:2486/{did}"))
                .header(header::USER_AGENT, "backshots-backfill/0.1")
                .body(body_empty())?,
        )
        .await?;
        if !res.status().is_success() {
            anyhow::bail!("got error status for did:plc request: {:?}", res.status())
        }
        let body = res.into_body().collect().await?.to_bytes();
        let body = String::from_utf8(body.to_vec())?;
        body.parse()?
    } else if did.starts_with("did:web:") {
        let authority = did.strip_prefix("did:web:").unwrap();
        let req = Request::builder()
            .uri(format!("https://{authority}/.well-known/did.json"))
            .header(header::USER_AGENT, "backshots-backfill/0.1")
            .header(header::HOST, authority)
            .body(body_empty())?;
        assert_eq!(
            req.uri().authority().map(|s| s.as_str()),
            Some(authority),
            "did:web tried to sneak in a path or something"
        );
        let res = tokio::select! {
            biased;
            res = fetch(req) => res,
            () = tokio::time::sleep(Duration::from_millis(5_000)) => {
                anyhow::bail!("did:web request took too long!")
            }
        }?;
        if !res.status().is_success() {
            anyhow::bail!("got error status for did:web request: {:?}", res.status())
        }
        let body = http_body_util::Limited::new(res.into_body(), 65_536);
        let body = body
            .collect()
            .await
            .map_err(anyhow::Error::from_boxed)?
            .to_bytes();
        let body = String::from_utf8(body.to_vec())?;
        body.parse()?
    } else {
        anyhow::bail!("unsupported did type")
    };

    Ok(did_doc)
}

async fn handle_queue_entry(
    app: &mut AppContext,
    storage: &mut LiveStorageWriter,
    did_id: u64,
    since: Option<String>,
) -> Result<String> {
    let did = resolve_did(app, did_id)?;
    tracing::info!(%did, ?since, "ingesting repo");

    let did_doc = get_did_document(&did).await?;
    let JsonValue::Array(service) = &did_doc["service"] else {
        anyhow::bail!("did doc `service` was not array")
    };
    let Some(JsonValue::String(ref service_endpoint)) = service
        .iter()
        .find(|e| {
            let JsonValue::String(ref id) = e["id"] else {
                return false;
            };
            id == "#atproto_pds"
        })
        .map(|val| &val["serviceEndpoint"])
    else {
        anyhow::bail!("could not find AtprotoPersonalDataServer")
    };

    let res = {
        let mut uri = format!("{service_endpoint}/xrpc/com.atproto.sync.getRepo?did={did}");
        if let Some(since) = since {
            uri.push_str("&since=");
            uri.push_str(&since);
        }
        let req = Request::builder()
            .uri(uri)
            .header(
                header::HOST,
                service_endpoint
                    .strip_prefix("https://")
                    .unwrap_or(service_endpoint),
            )
            .header(header::USER_AGENT, "backshots-backfill/0.1")
            .body(body_empty())?;

        tracing::debug!(?req);
        fetch(req).await?
    };
    if !res.status().is_success() {
        anyhow::bail!("got error response for getRepo: {:?}", res)
    }
    let repo = res.into_body().collect().await?;
    let mut repo = repo.to_bytes();
    let mut repo_cursor = std::io::Cursor::new(&mut repo);
    let rev = ingest_repo_archive(app, storage, did, &mut repo_cursor)?;

    Ok(rev)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().compact())
        .with(
            "backfill=debug,backshots=debug,backshots::ingest=info"
                .parse::<EnvFilter>()
                .unwrap(),
        )
        .init();

    let cfg = get_app_config()?;
    let mut app = AppContext::new(&cfg)?;

    let backfill_db = open_backfill_db(&cfg)?;

    let mut create_outdated =
        backfill_db.prepare("INSERT OR IGNORE INTO repos (did) VALUES (?)")?;
    for zplc in 1..=1_000 {
        let _did = resolve_did(&app, zplc)?;
        let _ = create_outdated.execute([zplc])?;
    }

    // TODO: parallelize (low key we can do as many concurrent requests as there are PDSes)
    let mut query_for_row =
        backfill_db.prepare("SELECT did, since FROM repos WHERE status = 'outdated' LIMIT 1")?;
    let mut update_row_status = backfill_db.prepare("UPDATE repos SET status = ? WHERE did = ?")?;
    let mut update_since = backfill_db.prepare("UPDATE repos SET since = ? WHERE did = ?")?;

    loop {
        let (did_id, since) = query_for_row.query_row((), |row| {
            Ok((row.get::<_, u64>(0)?, row.get::<_, Option<String>>(1)?))
        })?;
        update_row_status.execute((did_id, "processing"))?;
        let mut storage = LiveWriteHandle::latest(&app)?;
        match handle_queue_entry(&mut app, &mut storage, did_id, since).await {
            Ok(rev) => {
                update_row_status.execute(("done", did_id))?;
                update_since.execute((rev, did_id))?;
            }
            Err(err) => {
                tracing::warn!(?err, "an error occurred while backfilling a repo");
                update_row_status.execute(("errored", did_id))?;
            }
        }
    }
}
