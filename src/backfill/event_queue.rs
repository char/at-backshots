use std::io::Cursor;

use anyhow::Result;

use crate::{
    car::read_car_v1,
    firehose::{handle_commit, subscribe_repos::SubscribeReposCommit},
    mst::SignedCommitNode,
    storage::live::LiveStorageWriter,
    AppContext,
};

use super::db::convert_did_to_db;

pub fn flush_event_queue(
    app: &mut AppContext,
    storage: &mut LiveStorageWriter,
    backfill_db: &rusqlite::Connection,
    did: u64,
    ingested_rev: &str,
) -> Result<()> {
    let mut get_events =
        backfill_db.prepare_cached("DELETE FROM event_queue WHERE did = ? RETURNING event")?;

    let results = get_events.query_map([convert_did_to_db(did)], |row| {
        let value_ref = row.get_ref(0)?;
        let bytes = value_ref.as_bytes()?;
        Ok(serde_ipld_dagcbor::from_slice::<SubscribeReposCommit>(
            bytes,
        ))
    })?;

    for result in results {
        let result = result?;
        let Ok(commit) = result else {
            continue;
        };

        let mut cursor = Cursor::new(commit.blocks);
        let reader = &mut cursor;
        let car_file = read_car_v1(reader)?;

        let commit_block = car_file.read_block(reader, &commit.commit)?;
        let commit_node = serde_ipld_dagcbor::from_slice::<SignedCommitNode>(&commit_block)?;
        if commit_node.data.rev.as_str() < ingested_rev {
            continue;
        }

        if let Err(e) = handle_commit(
            app,
            storage,
            commit.repo,
            reader,
            &car_file,
            commit.operations,
        ) {
            tracing::error!("{e:?}")
        }
    }

    Ok(())
}
