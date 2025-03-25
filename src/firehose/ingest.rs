use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::StreamExt;
use hyper::header::HeaderValue;
use ipld_core::{cid::Cid, ipld::Ipld};
use serde_ipld_dagcbor::DecodeError;
use std::{collections::BTreeMap, io::Cursor, time::Duration};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::ingest::carslice::handle_carslice;
use crate::storage::live_guards::LiveStorageWriterGuard;
use crate::AppContext;
use crate::{car::read_car_v1, storage::live::LiveStorageWriter};

use super::subscribe_repos::{StreamEventHeader, SubscribeReposCommit, SubscribeReposInfo};

pub async fn ingest_firehose(
    app: &mut AppContext,
    domain: &str,
    port: u16,
    tls: bool,
) -> Result<()> {
    let mut storage = tokio::task::block_in_place(|| LiveStorageWriterGuard::latest(app))?;
    let mut event_count: u8 = 0;

    'reconnect: loop {
        let cursor = {
            app.db
                .query_row(
                    "SELECT count FROM counts WHERE key = 'firehose_cursor'",
                    (),
                    |row| row.get::<_, u64>(0),
                )
                .ok()
        };

        let firehose_path = format!(
            "/xrpc/com.atproto.sync.subscribeRepos{}",
            cursor.map(|c| format!("?cursor={c}")).unwrap_or_default()
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

        let mut cursor = cursor.unwrap_or_default();

        loop {
            let response = match tokio::time::timeout(Duration::from_secs(30), ws.next()).await {
                Ok(response) => response,
                Err(_timeout) => {
                    tracing::info!("websocket stream went quiet, reconnecting");
                    let _ = ws.close(None).await;
                    continue 'reconnect;
                }
            };
            match response {
                Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(bytes))) => {
                    event_count += 1;
                    if event_count % 128 == 0 {
                        event_count = 0;
                        if LiveStorageWriterGuard::latest_id(app).ok() != Some(storage.store_id) {
                            tracing::info!("rolling over live storage");

                            storage =
                                tokio::task::block_in_place(|| LiveStorageWriterGuard::latest(app))?
                        }
                    }

                    tokio::task::block_in_place(|| {
                        handle_event(app, &mut storage, bytes, &mut cursor)
                    })?;
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
                    tracing::error!("got error from websocket stream: {e:?}");
                    let _ = ws.close(None).await;
                    break 'reconnect;
                }
                None => {
                    tracing::info!("got no response from websocket stream, reconnecting");
                    let _ = ws.close(None).await;
                    continue 'reconnect;
                }
            }
        }
    }

    Ok(())
}

fn handle_event(
    app: &mut AppContext,
    storage: &mut LiveStorageWriter,
    event: Bytes,
    cursor_ref: &mut u64,
) -> Result<()> {
    let buf: &[u8] = &event;
    let mut buf_cur = Cursor::new(buf);
    let (header_buf, payload_buf) = match serde_ipld_dagcbor::from_reader::<Ipld, _>(&mut buf_cur) {
        Err(DecodeError::TrailingData) => buf.split_at(buf_cur.position() as usize),
        _ => anyhow::bail!("invalid sync frame format"),
    };

    let header = serde_ipld_dagcbor::from_slice::<StreamEventHeader>(header_buf)?;

    match header.t.as_deref() {
        Some("#commit") => {
            let commit = serde_ipld_dagcbor::from_slice::<SubscribeReposCommit>(payload_buf)?;
            if commit.sequence as u64 <= *cursor_ref {
                return Ok(());
            }

            {
                let cursor = commit.sequence as u64;
                app.db.execute(
                    "INSERT OR REPLACE INTO COUNTS (key, count) VALUES ('firehose_cursor', ?)",
                    [cursor],
                )?;
                *cursor_ref = cursor;
            }

            let mut cursor = Cursor::new(commit.blocks);
            let reader = &mut cursor;
            let car_file = read_car_v1(reader)?;

            let mut records = BTreeMap::<Cid, String>::new();
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

            if !records.is_empty() {
                if let Err(e) =
                    handle_carslice(app, storage, commit.repo, reader, &car_file, &records)
                {
                    tracing::error!("{:?}", e);
                };
            }
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
