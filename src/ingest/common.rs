use anyhow::Result;
use std::collections::HashSet;

use crate::{
    data::{
        at_uri::parse_at_uri,
        did::encode_did,
        record::{encode_collection, encode_rkey, RecordId},
    },
    storage::live_writer::LiveStorageWriter,
    AppContext,
};

pub fn handle_backlinks(
    app: &mut AppContext,
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
        encode_did(app, repo)?,
        encode_collection(app, collection)?,
        encode_rkey(app, rkey)?,
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
        fn create_record_id(
            app: &mut AppContext,
            did: &str,
            collection: &str,
            rkey: &str,
        ) -> Result<RecordId> {
            Ok(RecordId::new(
                encode_did(app, did)?,
                encode_collection(app, collection)?,
                encode_rkey(app, rkey)?,
            ))
        }

        match create_record_id(app, target_repo, target_collection, target_rkey) {
            Ok(target) => {
                tracing::debug!(from = source_display, to = uri, "backlink");

                // TODO: we probably shouldnt block the runtime like this but whatever
                storage.write_backlink(&target, &source)?;
                app.backlinks_counter.add(1);
            }
            Err(e) => tracing::warn!("failed to create RecordId: {:?}", e),
        };
    }

    Ok(())
}
