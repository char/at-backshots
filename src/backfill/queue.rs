use std::time::Duration;

use crate::{
    data::did::resolve_did,
    http::{body_empty, client::fetch},
    ingest::repo_car::ingest_repo_archive,
    storage::live::LiveStorageWriter,
    AppContext,
};
use anyhow::Result;
use http_body_util::BodyExt;
use hyper::{header, Request};
use tinyjson::JsonValue;

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

pub async fn handle_queue_entry(
    app: &mut AppContext,
    storage: &mut LiveStorageWriter,
    did: u64,
    since: Option<String>,
) -> Result<String> {
    let did_string = resolve_did(app, did)?;
    tracing::info!(did = %did_string, ?since, "ingesting repo");

    let did_doc = get_did_document(&did_string).await?;
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
        let mut uri = format!("{service_endpoint}/xrpc/com.atproto.sync.getRepo?did={did_string}");
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
    let rev = ingest_repo_archive(app, storage, did_string.clone(), &mut repo_cursor)?;

    // TODO: flush event queue

    tracing::info!(did = %did_string, %rev, "finished ingesting repo");

    Ok(rev)
}
