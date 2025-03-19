use anyhow::Result;
use std::collections::HashSet;

use crate::{
    data::{at_uri::parse_at_uri, record::RecordId},
    storage::BacklinkStorage,
    AppState,
};

pub async fn handle_backlinks(
    app: &AppState,
    storage: &mut BacklinkStorage,
    repo: &str,
    collection: &str,
    rkey: &str,
    backlinks: HashSet<(/* cid */ &str, /* uri */ &str)>,
) -> Result<()> {
    if backlinks.is_empty() {
        return Ok(());
    }

    let source = RecordId {
        did: app.encode_did(repo).await?.into(),
        collection: app.encode_collection(collection)?.into(),
        rkey: app.encode_rkey(rkey)?,
    };

    let source_display = format!("at://{repo}/{collection}/{rkey}");

    for (_cid, uri) in backlinks {
        let (target_repo, target_collection, target_rkey) = match parse_at_uri(uri) {
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
                did: app.encode_did(repo).await?.into(),
                collection: app.encode_collection(collection)?.into(),
                rkey: app.encode_rkey(rkey)?,
            })
        }

        match create_record_id(app, target_repo, target_collection, target_rkey).await {
            Ok(target) => {
                tracing::debug!(from = source_display, to = uri, "backlink");

                // TODO: we probably shouldnt block the runtime like this but whatever
                storage.store_backlink(&target, &source)?;

                app.incr_backlink_count(1)?;
                // app.db_records.merge(&target_bytes, &source_bytes)?;
            }
            Err(e) => tracing::warn!("failed to create RecordId: {:?}", e),
        };
    }

    Ok(())
}
