use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::StreamExt;
use hyper::header::HeaderValue;
use ipld_core::{cid::Cid, ipld::Ipld};
use serde_ipld_dagcbor::DecodeError;
use std::io::{Read, Seek};
use std::{collections::BTreeMap, io::Cursor, time::Duration};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::backfill::db::convert_did_to_db;
use crate::car::CarFile;
use crate::data::did::{encode_did, encode_existing_did};
use crate::ingest::carslice::handle_carslice;
use crate::mst::SignedCommitNode;
use crate::storage::live_guards::LiveWriteHandle;
use crate::AppContext;
use crate::{car::read_car_v1, storage::live::LiveStorageWriter};

use super::subscribe_repos::{
    RepoOperation, StreamEventHeader, SubscribeReposCommit, SubscribeReposInfo,
};

pub async fn ingest_firehose(
    app: &mut AppContext,
    backfill_db: Option<&rusqlite::Connection>,
    domain: &str,
    port: u16,
    tls: bool,
) -> Result<()> {
    let mut storage = tokio::task::block_in_place(|| LiveWriteHandle::latest(app))?;
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
                        if LiveWriteHandle::latest_id(app).ok() != Some(storage.store_id) {
                            tracing::info!("rolling over live storage");

                            storage = tokio::task::block_in_place(|| LiveWriteHandle::latest(app))?
                        }
                    }

                    tokio::task::block_in_place(|| {
                        handle_event(app, &mut storage, backfill_db, bytes, &mut cursor)
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

pub fn ingest_commit<R: Read + Seek>(
    app: &mut AppContext,
    storage: &mut LiveStorageWriter,
    repo: String,
    reader: &mut R,
    car_file: &CarFile,
    operations: Vec<RepoOperation>,
) -> Result<()> {
    let mut records = BTreeMap::<Cid, String>::new();
    for op in operations {
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

    if records.is_empty() {
        return Ok(());
    }

    handle_carslice(app, storage, repo, reader, car_file, &records)
}

fn handle_event(
    app: &mut AppContext,
    storage: &mut LiveStorageWriter,
    backfill_db: Option<&rusqlite::Connection>,
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

            if let Some(backfill_db) = backfill_db {
                let repo = commit.repo;
                let did_id = if repo.starts_with("did:plc:") {
                    match encode_existing_did(app, &repo)? {
                        Some(d) => d,
                        None => {
                            // skip super duper fresh did:plc identities that zplc doesn't know about yet
                            return Ok(());
                        }
                    }
                } else {
                    encode_did(app, &repo)?
                };

                let repo_status: String = {
                    let mut create_or_get_status = backfill_db.prepare_cached(
                        "INSERT INTO repos (did, status)
                    VALUES (?1, 'outdated')
                    ON CONFLICT(did) DO UPDATE SET
                        status = repos.status
                    RETURNING status",
                    )?;
                    create_or_get_status.query_row([convert_did_to_db(did_id)], |row| row.get(0))?

                    /* let mut get_status =
                        backfill_db.prepare_cached("SELECT status FROM repos WHERE did = ?")?;
                    match get_status.query_row([convert_did_to_db(did_id)], |row| row.get(0)) {
                        Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(()),
                        res => res,
                    }? */
                };
                match repo_status.as_str() {
                    "outdated" | "errored" => {
                        // drop events until we're processing
                        return Ok(());
                    }
                    "processing" => {
                        backfill_db.execute(
                            "INSERT INTO event_queue (did, event) VALUES (?, ?)",
                            (convert_did_to_db(did_id), payload_buf),
                        )?;
                    }
                    "done" => {
                        let mut get_last_rev =
                            backfill_db.prepare_cached("SELECT rev FROM repos WHERE did = ?")?;
                        let last_rev: String = get_last_rev
                            .query_row([convert_did_to_db(did_id)], |row| row.get(0))?;

                        let mut cursor = Cursor::new(commit.blocks);
                        let reader = &mut cursor;
                        let car_file = read_car_v1(reader)?;

                        let commit_block = car_file.read_block(reader, &commit.commit)?;
                        let commit_node =
                            serde_ipld_dagcbor::from_slice::<SignedCommitNode>(&commit_block)?;
                        if commit_node.data.rev.as_str() <= last_rev.as_str() {
                            return Ok(());
                        }

                        backfill_db.execute(
                            "UPDATE repos SET rev = ?2 WHERE did = ?1",
                            (convert_did_to_db(did_id), commit_node.data.rev),
                        )?;

                        if let Err(e) =
                            ingest_commit(app, storage, repo, reader, &car_file, commit.operations)
                        {
                            tracing::error!("{e:?}");
                        }
                    }
                    _ => anyhow::bail!("unknown repo status {repo_status}"),
                }
            } else {
                let mut cursor = Cursor::new(commit.blocks);
                let reader = &mut cursor;
                let car_file = read_car_v1(reader)?;
                if let Err(e) = ingest_commit(
                    app,
                    storage,
                    commit.repo,
                    reader,
                    &car_file,
                    commit.operations,
                ) {
                    tracing::error!("{e:?}");
                }
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
