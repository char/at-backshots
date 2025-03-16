use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::StreamExt;
use hyper::header::HeaderValue;
use ipld_core::{cid::Cid, ipld::Ipld};
use iroh_car::CarReader;
use serde_ipld_dagcbor::DecodeError;
use std::{
    collections::{HashMap, HashSet},
    io::Cursor,
    time::Duration,
};
use tokio::io::AsyncRead;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::{
    data::{at_uri::parse_at_uri, record::RecordId},
    lexicons::{StreamEventHeader, SubscribeReposCommit, SubscribeReposInfo},
    AppState,
};

pub async fn ingest(app: &AppState, domain: &str, port: u16, tls: bool) -> Result<()> {
    let cursor_path = format!("firehose_cursor/{domain}:{port}");
    // let _ = app.db.remove(&cursor_path)?;

    'reconnect: loop {
        let last_cursor = app.db.get(&cursor_path)?.map(|v| {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&v[0..8]);
            u64::from_le_bytes(bytes)
        });

        let firehose_path = format!(
            "/xrpc/com.atproto.sync.subscribeRepos{}",
            last_cursor
                .map(|c| format!("?cursor={c}"))
                .unwrap_or_default()
        );

        tracing::info!(%domain, "connecting to ingest…");

        let protocol = if tls { "wss" } else { "ws" };
        let mut req =
            format!("{protocol}://{domain}:{port}{firehose_path}").into_client_request()?;
        req.headers_mut()
            .insert("host", HeaderValue::from_str(domain).unwrap());
        let (mut ws, _res) = tokio_tungstenite::connect_async(req)
            .await
            .context("failed to connect websocket")?;

        loop {
            match ws.next().await {
                Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(bytes))) => {
                    handle_event(app, bytes).await?;
                }
                Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_close_frame))) => {
                    tracing::warn!("got close frame. reconnecting in 10s");
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    continue 'reconnect;
                }
                Some(Ok(msg)) => {
                    tracing::warn!("unexpected frame type {:?}", msg);
                }
                Some(Err(e)) => {
                    tracing::error!("{e:?}");
                    break 'reconnect;
                }
                None => {
                    break 'reconnect;
                }
            }
        }
    }

    Ok(())
}

async fn handle_event(app: &AppState, event: Bytes) -> Result<()> {
    let buf: &[u8] = &event;
    let mut cursor = Cursor::new(buf);
    let (header_buf, payload_buf) = match serde_ipld_dagcbor::from_reader::<Ipld, _>(&mut cursor) {
        Err(DecodeError::TrailingData) => buf.split_at(cursor.position() as usize),
        _ => anyhow::bail!("invalid sync frame format"),
    };

    let header = serde_ipld_dagcbor::from_slice::<StreamEventHeader>(header_buf)?;

    match header.t.as_deref() {
        Some("#commit") => {
            let commit = serde_ipld_dagcbor::from_slice::<SubscribeReposCommit>(payload_buf)?;

            let new_cursor = (commit.sequence as u64).to_le_bytes();
            app.db.insert(b"firehose_cursor", &new_cursor)?;

            let mut cursor = Cursor::new(commit.blocks);
            let reader = CarReader::new(&mut cursor).await?;

            let mut records = HashMap::<Cid, String>::new();
            for op in commit.operations {
                match op.action.as_str() {
                    "create" | "update" => {
                        let Some(cid) = op.cid else {
                            continue;
                        };
                        records.insert(cid, op.path);
                    }
                    "delete" => {
                        // TODO: handle deletes? this would be like a full scan every time :/
                    }
                    _ => tracing::warn!("unknown op action: {}", &op.action),
                }
            }

            if let Err(e) = handle_carslice(app, commit.repo, reader, records).await {
                tracing::error!("{:?}", e);
            };
        }
        Some("#info") => {
            let payload = serde_ipld_dagcbor::from_slice::<SubscribeReposInfo>(payload_buf)?;
            if payload.name == "OutdatedCursor" {
                tracing::warn!(message = ?payload.message, "outdated cursor");
            }
        }
        _ => {}
    }

    Ok(())
}

pub async fn handle_carslice<R: AsyncRead + Unpin>(
    app: &AppState,
    repo: String,
    mut car_reader: CarReader<R>,
    mut records: HashMap<Cid, String>,
) -> Result<()> {
    while let Some((cid, cbor)) = car_reader.next_block().await? {
        let Some(path) = records.remove(&cid) else {
            continue;
        };
        let Some((collection, rkey)) = path.split_once('/') else {
            continue;
        };
        let ipld = serde_ipld_dagcbor::from_slice::<Ipld>(&cbor)?;
        // dbg!(&repo, &collection, &rkey, &ipld);

        let mut backlinks = HashSet::<(&str, &str)>::new();

        for child in ipld.iter() {
            // a StrongRef is an Ipld::Map with "cid" and "uri"
            let Ipld::Map(map) = child else {
                continue;
            };
            if let (Some(Ipld::String(cid)), Some(Ipld::String(uri))) =
                (map.get("cid"), map.get("uri"))
            {
                backlinks.insert((cid, uri));
            }
        }

        handle_backlinks(app, &repo, collection, rkey, backlinks).await?;
    }

    if !records.is_empty() {
        tracing::warn!(
            "got leftover records while handling event: {:?}",
            records
                .keys()
                .map(|c| c
                    .to_string_of_base(multibase::Base::Base32Lower)
                    .unwrap_or_else(|_| "<error>".into()))
                .collect::<Vec<_>>()
        )
    }

    Ok(())
}

async fn handle_backlinks(
    app: &AppState,
    repo: &str,
    collection: &str,
    rkey: &str,
    backlinks: HashSet<(/* cid */ &str, /* uri */ &str)>,
) -> Result<()> {
    if backlinks.is_empty() {
        return Ok(());
    }

    let source = RecordId {
        did: app.encode_did(repo).await?,
        collection: app.encode_collection(collection)?,
        rkey: app.encode_rkey(rkey)?,
    };

    let mut targets = Vec::<RecordId>::with_capacity(backlinks.len());
    for (_cid, uri) in backlinks {
        let (repo, collection, rkey) = match parse_at_uri(uri) {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("failed to parse at uri {uri}: {:?}", e);
                continue;
            }
        };

        // my kingdom for a try block
        async fn create_record_id(
            app: &AppState,
            repo: &str,
            collection: &str,
            rkey: &str,
        ) -> Result<RecordId> {
            Ok(RecordId {
                did: app.encode_did(repo).await?,
                collection: app.encode_collection(collection)?,
                rkey: app.encode_rkey(rkey)?,
            })
        }

        match create_record_id(app, repo, collection, rkey).await {
            Ok(rid) => targets.push(rid),
            Err(e) => tracing::warn!("failed to create RecordId: {:?}", e),
        };
    }

    let source_bytes = unsafe {
        let ptr = &raw const source as *const u8;
        std::slice::from_raw_parts(ptr, std::mem::size_of::<RecordId>())
    };
    let targets_bytes = unsafe {
        std::slice::from_raw_parts(
            targets.as_ptr() as *const u8,
            targets.len() * std::mem::size_of::<RecordId>(),
        )
    };
    app.db_records.merge(source_bytes, targets_bytes)?;

    let source_display = source.to_string(app).await?;
    for target in targets.iter() {
        let target_display = target.to_string(app).await?;
        tracing::debug!("{} -> {}", source_display, target_display);
    }

    Ok(())
}
