use anyhow::Result;
use std::collections::HashSet;

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

    let source = RecordId {
        did: app.encode_did(repo).await?,
        collection: app.encode_collection(collection)?,
        rkey: app.encode_rkey(rkey)?,
    };

    let source_display = format!("at://{repo}/{collection}/{rkey}");
    let source_bytes = unsafe {
        let ptr = &raw const source as *const u8;
        std::slice::from_raw_parts(ptr, std::mem::size_of::<RecordId>())
    };

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
                did: app.encode_did(repo).await?,
                collection: app.encode_collection(collection)?,
                rkey: app.encode_rkey(rkey)?,
            })
        }

        match create_record_id(app, target_repo, target_collection, target_rkey).await {
            Ok(target) => {
                tracing::debug!(from = source_display, to = uri, "backlink");

                let target_bytes = unsafe {
                    let ptr = &raw const target as *const u8;
                    std::slice::from_raw_parts(ptr, std::mem::size_of::<RecordId>())
                };
                app.db_records.merge(target_bytes, source_bytes)?;
            }
            Err(e) => tracing::warn!("failed to create RecordId: {:?}", e),
        };
    }

    Ok(())
}
