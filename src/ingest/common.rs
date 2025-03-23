use anyhow::Result;
use std::collections::HashSet;

use crate::{
    data::{at_uri::parse_at_uri, record::RecordId},
    storage::live_writer::LiveStorageWriter,
    AppState,
};

pub async fn handle_backlinks(
    app: &AppState,
    storage: &mut LiveStorageWriter,
    repo: &str,
    collection: &str,
    rkey: &str,
    backlinks: HashSet<(/* cid */ &str, /* uri */ &str)>,
) -> Result<()> {
    if backlinks.is_empty() {
        return Ok(());
    }

    let source = RecordId::new(
        app.encode_did(repo).await?,
        app.encode_collection(collection)?,
        app.encode_rkey(rkey)?,
    );

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
        #[inline]
        async fn create_record_id(
            app: &AppState,
            did: &str,
            collection: &str,
            rkey: &str,
        ) -> Result<RecordId> {
            Ok(RecordId::new(
                app.encode_did(did).await?,
                app.encode_collection(collection)?,
                app.encode_rkey(rkey)?,
            ))
        }

        #[inline]
        fn try_create_record_id_sync(
            app: &AppState,
            did: &str,
            collection: &str,
            rkey: &str,
        ) -> Result<RecordId> {
            Ok(RecordId::new(
                app.try_encode_did_sync(did)
                    .ok_or_else(|| anyhow::anyhow!("cant encode did"))?,
                app.encode_collection(collection)?,
                app.encode_rkey(rkey)?,
            ))
        }

        let record_id =
            match try_create_record_id_sync(app, target_repo, target_collection, target_rkey) {
                Ok(target) => Ok(target),
                Err(_) => create_record_id(app, repo, collection, rkey).await,
            };

        match record_id {
            Ok(target) => {
                tracing::debug!(from = source_display, to = uri, "backlink");

                // TODO: we probably shouldnt block the runtime like this but whatever
                storage.write_backlink(&target, &source)?;
                app.incr_backlink_count(1)?;
            }
            Err(e) => tracing::warn!("failed to create RecordId: {:?}", e),
        };
    }

    Ok(())
}
