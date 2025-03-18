use anyhow::Result;
use std::collections::HashSet;
use zerocopy::IntoBytes;

use crate::{
    data::{at_uri::parse_at_uri, record::RecordId},
    AppState,
};

pub async fn handle_backlinks(
    app: &AppState,
    repo: &str,
    collection: &str,
    rkey: &str,
    backlinks: HashSet<(/* cid */ &str, /* uri */ &str)>,
) -> Result<()> {
    if backlinks.is_empty() {
        return Ok(());
    }

    let mut source = RecordId {
        did: app.encode_did(repo).await?.into(),
        collection: app.encode_collection(collection)?.into(),
        rkey: app.encode_rkey(rkey)?,
    };

    let source_display = format!("at://{repo}/{collection}/{rkey}");
    let source_bytes = source.as_mut_bytes();

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
            Ok(mut target) => {
                tracing::debug!(from = source_display, to = uri, "backlink");

                let target_bytes = target.as_mut_bytes();
                app.db_records.merge(&target_bytes, &source_bytes)?;
                app.incr_backlink_count(1)?;
            }
            Err(e) => tracing::warn!("failed to create RecordId: {:?}", e),
        };
    }

    Ok(())
}
