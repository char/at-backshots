use std::time::Duration;

use anyhow::Result;
use backshots::{
    backfill::{
        db::{convert_did_from_db, convert_did_to_db, open_backfill_db},
        event_queue::flush_event_queue,
        repo::fetch_and_ingest_repo,
    },
    get_app_config,
    storage::live_guards::LiveWriteHandle,
    AppContext,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

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

    let mut query_for_row = backfill_db.prepare(
        "UPDATE repos
        SET status = 'processing'
        WHERE id in (SELECT id FROM repos
            WHERE status = 'outdated'
            ORDER BY updated ASC
            LIMIT 1)
        RETURNING did, rev",
    )?;
    let mut update_row_status = backfill_db.prepare(
        "UPDATE repos SET status = ?, updated = unixepoch('now', 'subsec') WHERE did = ?",
    )?;
    let mut update_rev = backfill_db.prepare("UPDATE repos SET rev = ? WHERE did = ?")?;

    // TODO: parallelize (low key we can do as many concurrent requests as there are PDSes)
    loop {
        let row_result = query_for_row.query_row((), |row| {
            Ok((
                row.get(0).map(convert_did_from_db)?,
                row.get::<_, Option<String>>(1)?,
            ))
        });
        let (did, rev) = match row_result {
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                tokio::time::sleep(Duration::from_millis(1_000)).await;
                continue;
            }
            r => r,
        }?;

        let mut storage = LiveWriteHandle::latest(&app)?;
        match fetch_and_ingest_repo(&mut app, &mut storage, did, rev).await {
            Ok(rev) => {
                tokio::task::block_in_place(|| {
                    flush_event_queue(&mut app, &mut storage, &backfill_db, did, &rev)
                })?;
                update_row_status.execute(("done", convert_did_to_db(did)))?;
                update_rev.execute((rev, convert_did_to_db(did)))?;
            }
            Err(err) => {
                // TODO: clear event queue

                tracing::warn!(?err, "an error occurred while backfilling a repo");
                update_row_status.execute(("errored", did))?;
            }
        }
    }
}
