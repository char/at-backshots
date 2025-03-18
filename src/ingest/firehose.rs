use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::StreamExt;
use hyper::header::HeaderValue;
use ipld_core::{cid::Cid, ipld::Ipld};
use serde_ipld_dagcbor::DecodeError;
use std::{collections::BTreeMap, io::Cursor, time::Duration};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::{
    car::read_car_v1,
    lexicons::{StreamEventHeader, SubscribeReposCommit, SubscribeReposInfo},
    AppState,
};

use super::carslice::handle_carslice;

pub async fn ingest_firehose(app: &AppState, domain: &str, port: u16, tls: bool) -> Result<()> {
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
            let reader = &mut cursor;
            let car_file = read_car_v1(reader).await?;

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

            if let Err(e) = handle_carslice(app, commit.repo, reader, &car_file, &records).await {
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
